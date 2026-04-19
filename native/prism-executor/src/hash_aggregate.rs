//! Hash aggregation on Arrow RecordBatches.
//!
//! Replaces Trino's `FlatHash` / `BigintGroupByHash`. Groups rows by key columns
//! and computes aggregate functions using vectorized Arrow compute kernels.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::compute::kernels::aggregate as arrow_agg;
use arrow_array::{
    cast::AsArray, Array, ArrayRef, Float64Array, Int64Array, RecordBatch,
    types::{Date32Type, Float64Type, Int32Type, Int64Type},
};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rayon::prelude::*;

use crate::Result;

/// Fast non-cryptographic hasher (FxHash algorithm).
/// ~3-5x faster than DefaultHasher (SipHash) for integer keys — appropriate
/// for internal group-key hashing where cryptographic security is not required.
struct FxHasher {
    state: u64,
}

impl FxHasher {
    #[inline(always)]
    fn new() -> Self {
        Self { state: 0 }
    }
}

impl std::hash::Hasher for FxHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.state
    }

    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut buf = [0u8; 8];
            buf[..chunk.len()].copy_from_slice(chunk);
            let word = u64::from_le_bytes(buf);
            self.state = self.state.wrapping_mul(0x517cc1b727220a95).wrapping_add(word);
        }
    }

    #[inline(always)]
    fn write_u64(&mut self, i: u64) {
        self.state = self.state.wrapping_mul(0x517cc1b727220a95).wrapping_add(i);
    }

    #[inline(always)]
    fn write_i64(&mut self, i: i64) {
        self.write_u64(i as u64);
    }

    #[inline(always)]
    fn write_i32(&mut self, i: i32) {
        self.write_u64(i as u64);
    }

    #[inline(always)]
    fn write_u32(&mut self, i: u32) {
        self.write_u64(i as u64);
    }

    #[inline(always)]
    fn write_u8(&mut self, i: u8) {
        self.write_u64(i as u64);
    }
}

/// Supported aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    CountDistinct,
}

/// Definition of a single aggregation to compute.
#[derive(Debug, Clone)]
pub struct AggExpr {
    pub column: usize,
    pub func: AggFunc,
    pub output_name: String,
}

/// Configuration for hash aggregation.
#[derive(Debug, Clone)]
pub struct HashAggConfig {
    pub group_by: Vec<usize>,
    pub aggregates: Vec<AggExpr>,
}

#[derive(Debug, Clone)]
struct GroupAccumulator {
    sum: f64,
    count: u64,
    min: f64,
    max: f64,
    distinct_values: Option<HashSet<DistinctValue>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum DistinctValue {
    Int32(i32),
    Int64(i64),
    Float64(u64),
    Utf8(String),
    Boolean(bool),
    Date32(i32),
    Fallback(String),
}

impl GroupAccumulator {
    fn new(track_distinct: bool) -> Self {
        Self {
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            distinct_values: if track_distinct { Some(HashSet::new()) } else { None },
        }
    }

    #[inline(always)]
    fn accumulate(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
        if value < self.min { self.min = value; }
        if value > self.max { self.max = value; }
    }

    #[inline(always)]
    fn accumulate_distinct(&mut self, value: DistinctValue) {
        if let Some(ref mut dv) = self.distinct_values {
            dv.insert(value);
        }
    }

    fn merge(&mut self, other: &GroupAccumulator) {
        self.sum += other.sum;
        self.count += other.count;
        if other.min < self.min { self.min = other.min; }
        if other.max > self.max { self.max = other.max; }
        if let (Some(ref mut dv), Some(ref other_dv)) = (&mut self.distinct_values, &other.distinct_values) {
            for v in other_dv {
                dv.insert(v.clone());
            }
        }
    }

    fn result(&self, func: AggFunc) -> f64 {
        match func {
            AggFunc::Sum => self.sum,
            AggFunc::Count => self.count as f64,
            AggFunc::Avg => if self.count > 0 { self.sum / self.count as f64 } else { 0.0 },
            AggFunc::Min => if self.count > 0 { self.min } else { 0.0 },
            AggFunc::Max => if self.count > 0 { self.max } else { 0.0 },
            AggFunc::CountDistinct => self.distinct_values.as_ref().map_or(0, |v| v.len()) as f64,
        }
    }
}

/// Execute hash aggregation on a RecordBatch.
pub fn hash_aggregate(batch: &RecordBatch, config: &HashAggConfig) -> Result<RecordBatch> {
    hash_aggregate_batches(&[batch.clone()], config)
}

/// Execute hash aggregation across multiple RecordBatches without concatenating them.
///
/// This avoids the memory spike from `concat_batches` on large datasets by
/// streaming rows from each batch into the same hash map accumulators.
pub fn hash_aggregate_batches(batches: &[RecordBatch], config: &HashAggConfig) -> Result<RecordBatch> {
    // Fast path: global aggregates (no GROUP BY) — use Arrow SIMD kernels
    if config.group_by.is_empty() && !batches.is_empty() && batches.iter().any(|b| b.num_rows() > 0) {
        return global_aggregate_batches(batches, config);
    }

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        let schema = if batches.is_empty() {
            let mut fields: Vec<Arc<Field>> = Vec::new();
            for agg in &config.aggregates {
                let dt = match agg.func {
                    AggFunc::Count | AggFunc::CountDistinct => DataType::Int64,
                    _ => DataType::Float64,
                };
                fields.push(Arc::new(Field::new(&agg.output_name, dt, true)));
            }
            SchemaRef::new(Schema::new(fields))
        } else {
            batches[0].schema()
        };
        return build_empty_result(schema, config);
    }

    let ref_batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();

    // Parallel aggregation: partition batches across rayon threads,
    // each builds its own HashMap, then merge.
    let non_empty: Vec<(usize, &RecordBatch)> = batches.iter()
        .enumerate()
        .filter(|(_, b)| b.num_rows() > 0)
        .collect();

    // Each chunk produces: HashMap<group_hash, (batch_idx, row_idx, accumulators)>
    type GroupMap = HashMap<u64, (usize, usize, Vec<GroupAccumulator>)>;

    let chunk_size = (non_empty.len() / rayon::current_num_threads().max(1)).max(1);
    let partial_maps: Vec<(GroupMap, Vec<u64>)> = non_empty
        .par_chunks(chunk_size)
        .map(|chunk| {
            let mut groups: GroupMap = HashMap::new();
            let mut group_order: Vec<u64> = Vec::new();

            for &(batch_idx, batch) in chunk {
                let num_rows = batch.num_rows();

                let agg_arrays: Vec<Option<Float64Array>> = config.aggregates.iter()
                    .map(|agg| {
                        if agg.func == AggFunc::CountDistinct {
                            None
                        } else {
                            let col = batch.column(agg.column);
                            if col.data_type() == &DataType::Float64 {
                                Some(col.as_primitive::<Float64Type>().clone())
                            } else {
                                Some(
                                    cast(col, &DataType::Float64)
                                        .expect("cast to f64")
                                        .as_primitive::<Float64Type>()
                                        .clone()
                                )
                            }
                        }
                    })
                    .collect();

                for row in 0..num_rows {
                    let gh = hash_group_keys(batch, &config.group_by, row);
                    let entry = groups.entry(gh).or_insert_with(|| {
                        group_order.push(gh);
                        let accums = config.aggregates.iter()
                            .map(|agg| GroupAccumulator::new(agg.func == AggFunc::CountDistinct))
                            .collect();
                        (batch_idx, row, accums)
                    });

                    for (agg_idx, agg) in config.aggregates.iter().enumerate() {
                        if agg.func == AggFunc::CountDistinct {
                            if let Some(value) = distinct_value(batch.column(agg.column), row) {
                                entry.2[agg_idx].accumulate_distinct(value);
                            }
                            continue;
                        }
                        let value = agg_arrays[agg_idx]
                            .as_ref()
                            .expect("numeric aggregate must precompute cast array")
                            .value(row);
                        entry.2[agg_idx].accumulate(value);
                    }
                }
            }
            (groups, group_order)
        })
        .collect();

    // Merge partial maps into a single result
    let mut groups: GroupMap = HashMap::new();
    let mut group_order: Vec<u64> = Vec::new();

    for (partial, partial_order) in partial_maps {
        for gh in partial_order {
            if let Some((bi, ri, accums)) = partial.get(&gh) {
                let entry = groups.entry(gh).or_insert_with(|| {
                    group_order.push(gh);
                    let accums = config.aggregates.iter()
                        .map(|agg| GroupAccumulator::new(agg.func == AggFunc::CountDistinct))
                        .collect();
                    (*bi, *ri, accums)
                });
                // Merge accumulators
                for (agg_idx, acc) in accums.iter().enumerate() {
                    entry.2[agg_idx].merge(acc);
                }
            }
        }
    }

    // Build output
    let mut output_columns: Vec<ArrayRef> = Vec::new();
    let mut output_fields: Vec<Arc<Field>> = Vec::new();

    for &col_idx in &config.group_by {
        let field = ref_batch.schema().field(col_idx).clone();
        let mut values: Vec<ArrayRef> = Vec::new();
        for &gh in &group_order {
            let (bi, ri, _) = &groups[&gh];
            let source_col = batches[*bi].column(col_idx);
            values.push(source_col.slice(*ri, 1));
        }
        let refs: Vec<&dyn Array> = values.iter().map(|a| a.as_ref()).collect();
        let concatenated = arrow::compute::concat(&refs)?;
        output_fields.push(Arc::new(field));
        output_columns.push(concatenated);
    }

    for (agg_idx, agg) in config.aggregates.iter().enumerate() {
        let values: Vec<f64> = group_order.iter()
            .map(|gh| groups[gh].2[agg_idx].result(agg.func))
            .collect();

        let output_type = match agg.func {
            AggFunc::Count | AggFunc::CountDistinct => DataType::Int64,
            _ => DataType::Float64,
        };

        let array: ArrayRef = match output_type {
            DataType::Int64 => Arc::new(Int64Array::from(
                values.iter().map(|v| *v as i64).collect::<Vec<_>>()
            )),
            _ => Arc::new(Float64Array::from(values)),
        };

        output_fields.push(Arc::new(Field::new(&agg.output_name, output_type, true)));
        output_columns.push(array);
    }

    let schema = SchemaRef::new(Schema::new(output_fields));
    Ok(RecordBatch::try_new(schema, output_columns)?)
}

/// Fast path for global aggregates (no GROUP BY) using Arrow SIMD compute kernels.
/// Uses rayon for parallel processing across batches.
fn global_aggregate_batches(batches: &[RecordBatch], config: &HashAggConfig) -> Result<RecordBatch> {
    let mut output_fields: Vec<Arc<Field>> = Vec::new();
    let mut output_columns: Vec<ArrayRef> = Vec::new();

    for agg in &config.aggregates {
        let output_type = match agg.func {
            AggFunc::Count | AggFunc::CountDistinct => DataType::Int64,
            _ => DataType::Float64,
        };

        let result_value: f64 = match agg.func {
            AggFunc::Sum => {
                let partials: Vec<f64> = batches.par_iter()
                    .filter(|b| b.num_rows() > 0)
                    .map(|batch| {
                        let f64_col = cast_to_f64(batch.column(agg.column)).unwrap();
                        arrow_agg::sum(f64_col.as_primitive::<Float64Type>()).unwrap_or(0.0)
                    })
                    .collect();
                partials.iter().sum()
            }
            AggFunc::Count => {
                let partials: Vec<u64> = batches.par_iter()
                    .map(|batch| {
                        if agg.column >= batch.num_columns() {
                            // COUNT(*) on zero-column batch — count all rows
                            batch.num_rows() as u64
                        } else {
                            let col = batch.column(agg.column);
                            (col.len() - col.null_count()) as u64
                        }
                    })
                    .collect();
                partials.iter().sum::<u64>() as f64
            }
            AggFunc::Avg => {
                let partials: Vec<(f64, u64)> = batches.par_iter()
                    .filter(|b| b.num_rows() > 0)
                    .map(|batch| {
                        let col = batch.column(agg.column);
                        let f64_col = cast_to_f64(col).unwrap();
                        let s = arrow_agg::sum(f64_col.as_primitive::<Float64Type>()).unwrap_or(0.0);
                        let c = (col.len() - col.null_count()) as u64;
                        (s, c)
                    })
                    .collect();
                let total_sum: f64 = partials.iter().map(|(s, _)| s).sum();
                let total_count: u64 = partials.iter().map(|(_, c)| c).sum();
                if total_count > 0 { total_sum / total_count as f64 } else { 0.0 }
            }
            AggFunc::Min => {
                let partials: Vec<f64> = batches.par_iter()
                    .filter(|b| b.num_rows() > 0)
                    .filter_map(|batch| {
                        let f64_col = cast_to_f64(batch.column(agg.column)).ok()?;
                        arrow_agg::min(f64_col.as_primitive::<Float64Type>())
                    })
                    .collect();
                let global_min = partials.iter().cloned().fold(f64::INFINITY, f64::min);
                if global_min == f64::INFINITY { 0.0 } else { global_min }
            }
            AggFunc::Max => {
                let partials: Vec<f64> = batches.par_iter()
                    .filter(|b| b.num_rows() > 0)
                    .filter_map(|batch| {
                        let f64_col = cast_to_f64(batch.column(agg.column)).ok()?;
                        arrow_agg::max(f64_col.as_primitive::<Float64Type>())
                    })
                    .collect();
                let global_max = partials.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                if global_max == f64::NEG_INFINITY { 0.0 } else { global_max }
            }
            AggFunc::CountDistinct => {
                let partial_sets: Vec<HashSet<DistinctValue>> = batches.par_iter()
                    .map(|batch| {
                        let mut set = HashSet::new();
                        let col = batch.column(agg.column);
                        for i in 0..col.len() {
                            if let Some(value) = distinct_value(col, i) {
                                set.insert(value);
                            }
                        }
                        set
                    })
                    .collect();
                let mut merged: HashSet<DistinctValue> = HashSet::new();
                for set in partial_sets {
                    merged.extend(set);
                }
                merged.len() as f64
            }
        };

        let array: ArrayRef = match output_type {
            DataType::Int64 => Arc::new(Int64Array::from(vec![result_value as i64])),
            _ => Arc::new(Float64Array::from(vec![result_value])),
        };

        output_fields.push(Arc::new(Field::new(&agg.output_name, output_type, true)));
        output_columns.push(array);
    }

    let schema = SchemaRef::new(Schema::new(output_fields));
    Ok(RecordBatch::try_new(schema, output_columns)?)
}

fn cast_to_f64(array: &ArrayRef) -> Result<ArrayRef> {
    if array.data_type() == &DataType::Float64 {
        Ok(array.clone())
    } else {
        Ok(arrow_cast::cast(array, &DataType::Float64)?)
    }
}

fn distinct_value(array: &ArrayRef, row: usize) -> Option<DistinctValue> {
    if array.is_null(row) {
        return None;
    }

    Some(match array.data_type() {
        DataType::Int32 => DistinctValue::Int32(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => DistinctValue::Int64(array.as_primitive::<Int64Type>().value(row)),
        DataType::Float64 => {
            let value = array.as_primitive::<Float64Type>().value(row);
            let bits = if value == 0.0 {
                0.0f64.to_bits()
            } else if value.is_nan() {
                f64::NAN.to_bits()
            } else {
                value.to_bits()
            };
            DistinctValue::Float64(bits)
        }
        DataType::Utf8 => DistinctValue::Utf8(array.as_string::<i32>().value(row).to_owned()),
        DataType::Boolean => DistinctValue::Boolean(array.as_boolean().value(row)),
        DataType::Date32 => DistinctValue::Date32(array.as_primitive::<Date32Type>().value(row)),
        _ => DistinctValue::Fallback(format!("{:?}", array.slice(row, 1))),
    })
}

fn hash_group_keys(batch: &RecordBatch, key_cols: &[usize], row: usize) -> u64 {
    let mut hasher = FxHasher::new();
    for &col_idx in key_cols {
        let col = batch.column(col_idx);
        hash_array_value(col.as_ref(), row, &mut hasher);
    }
    hasher.finish()
}

fn hash_array_value(array: &dyn Array, row: usize, hasher: &mut impl Hasher) {
    if array.is_null(row) {
        0u8.hash(hasher);
        return;
    }
    match array.data_type() {
        DataType::Int32 => array.as_primitive::<Int32Type>().value(row).hash(hasher),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row).hash(hasher),
        DataType::Float64 => array.as_primitive::<Float64Type>().value(row).to_bits().hash(hasher),
        DataType::Utf8 => array.as_string::<i32>().value(row).hash(hasher),
        _ => format!("{:?}", array.slice(row, 1)).hash(hasher),
    }
}

fn build_empty_result(input_schema: SchemaRef, config: &HashAggConfig) -> Result<RecordBatch> {
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();

    for &col_idx in &config.group_by {
        let field = input_schema.field(col_idx).clone();
        columns.push(arrow_array::new_empty_array(field.data_type()));
        fields.push(Arc::new(field));
    }

    for agg in &config.aggregates {
        let dt = match agg.func {
            AggFunc::Count | AggFunc::CountDistinct => DataType::Int64,
            _ => DataType::Float64,
        };
        fields.push(Arc::new(Field::new(&agg.output_name, dt.clone(), true)));
        columns.push(arrow_array::new_empty_array(&dt));
    }

    let schema = SchemaRef::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;

    fn sales_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("product", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("qty", DataType::Int64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["east", "west", "east", "west", "east"])),
                Arc::new(StringArray::from(vec!["A", "B", "A", "A", "B"])),
                Arc::new(Float64Array::from(vec![100.0, 200.0, 150.0, 300.0, 50.0])),
                Arc::new(Int64Array::from(vec![10, 20, 15, 30, 5])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_sum_by_region() {
        let config = HashAggConfig {
            group_by: vec![0],
            aggregates: vec![AggExpr {
                column: 2,
                func: AggFunc::Sum,
                output_name: "total_amount".into(),
            }],
        };
        let result = hash_aggregate(&sales_batch(), &config).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2);
        let amounts = result.column(1).as_primitive::<Float64Type>();
        let total: f64 = (0..result.num_rows()).map(|i| amounts.value(i)).sum();
        assert_eq!(total, 800.0);
    }

    #[test]
    fn test_count_and_avg() {
        let config = HashAggConfig {
            group_by: vec![0],
            aggregates: vec![
                AggExpr { column: 2, func: AggFunc::Count, output_name: "cnt".into() },
                AggExpr { column: 2, func: AggFunc::Avg, output_name: "avg_amount".into() },
            ],
        };
        let result = hash_aggregate(&sales_batch(), &config).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 3);
    }

    #[test]
    fn test_global_count_distinct_preserves_large_int64_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![
                Some(9_007_199_254_740_992_i64),
                Some(9_007_199_254_740_993_i64),
                Some(9_007_199_254_740_992_i64),
                None,
            ]))],
        )
        .unwrap();

        let config = HashAggConfig {
            group_by: vec![],
            aggregates: vec![AggExpr {
                column: 0,
                func: AggFunc::CountDistinct,
                output_name: "distinct_ids".into(),
            }],
        };

        let result = hash_aggregate(&batch, &config).unwrap();
        let counts = result.column(0).as_primitive::<Int64Type>();
        assert_eq!(counts.value(0), 2);
    }

    #[test]
    fn test_grouped_count_distinct_supports_utf8() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("label", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["east", "east", "east", "west", "west", "west"])),
                Arc::new(StringArray::from(vec![
                    Some("A"),
                    Some("A"),
                    Some("B"),
                    Some("A"),
                    None,
                    Some("C"),
                ])),
            ],
        )
        .unwrap();

        let config = HashAggConfig {
            group_by: vec![0],
            aggregates: vec![AggExpr {
                column: 1,
                func: AggFunc::CountDistinct,
                output_name: "distinct_labels".into(),
            }],
        };

        let result = hash_aggregate(&batch, &config).unwrap();
        let groups = result.column(0).as_string::<i32>();
        let counts = result.column(1).as_primitive::<Int64Type>();

        let mut seen = HashMap::new();
        for row in 0..result.num_rows() {
            seen.insert(groups.value(row).to_owned(), counts.value(row));
        }

        assert_eq!(seen.get("east"), Some(&2));
        assert_eq!(seen.get("west"), Some(&2));
    }
}
