//! Hash aggregation on Arrow RecordBatches.
//!
//! Replaces Trino's `FlatHash` / `BigintGroupByHash`. Groups rows by key columns
//! and computes aggregate functions using vectorized Arrow compute kernels.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::{
    cast::AsArray, Array, ArrayRef, Float64Array, Int64Array, RecordBatch,
    types::{Float32Type, Float64Type, Int32Type, Int64Type, UInt64Type},
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{PrismError, Result};

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
    distinct_hashes: Option<Vec<u64>>,
}

impl GroupAccumulator {
    fn new(track_distinct: bool) -> Self {
        Self {
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            distinct_hashes: if track_distinct { Some(Vec::new()) } else { None },
        }
    }

    fn accumulate(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
        if value < self.min { self.min = value; }
        if value > self.max { self.max = value; }
        if let Some(ref mut dv) = self.distinct_hashes {
            let h = value.to_bits();
            if !dv.contains(&h) {
                dv.push(h);
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
            AggFunc::CountDistinct => self.distinct_hashes.as_ref().map_or(0, |v| v.len()) as f64,
        }
    }
}

/// Execute hash aggregation on a RecordBatch.
pub fn hash_aggregate(batch: &RecordBatch, config: &HashAggConfig) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return build_empty_result(batch.schema(), config);
    }

    // Map group hash → (first_row_index, per-agg accumulators)
    let mut groups: HashMap<u64, (usize, Vec<GroupAccumulator>)> = HashMap::new();
    let mut group_order: Vec<u64> = Vec::new();

    for row in 0..num_rows {
        let gh = hash_group_keys(batch, &config.group_by, row);
        let entry = groups.entry(gh).or_insert_with(|| {
            group_order.push(gh);
            let accums = config.aggregates.iter()
                .map(|agg| GroupAccumulator::new(agg.func == AggFunc::CountDistinct))
                .collect();
            (row, accums)
        });

        for (agg_idx, agg) in config.aggregates.iter().enumerate() {
            let value = get_numeric_value(batch.column(agg.column).as_ref(), row)?;
            entry.1[agg_idx].accumulate(value);
        }
    }

    // Build output
    let mut output_columns: Vec<ArrayRef> = Vec::new();
    let mut output_fields: Vec<Arc<Field>> = Vec::new();

    // Group-by columns
    for &col_idx in &config.group_by {
        let field = batch.schema().field(col_idx).clone();
        let source_col = batch.column(col_idx);
        let take_indices: Vec<u32> = group_order.iter()
            .map(|gh| groups[gh].0 as u32)
            .collect();
        let indices = arrow_array::UInt32Array::from(take_indices);
        let taken = arrow::compute::take(source_col, &indices, None)?;
        output_fields.push(Arc::new(field));
        output_columns.push(taken);
    }

    // Aggregate result columns
    for (agg_idx, agg) in config.aggregates.iter().enumerate() {
        let values: Vec<f64> = group_order.iter()
            .map(|gh| groups[gh].1[agg_idx].result(agg.func))
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

fn hash_group_keys(batch: &RecordBatch, key_cols: &[usize], row: usize) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
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

fn get_numeric_value(array: &dyn Array, row: usize) -> Result<f64> {
    match array.data_type() {
        DataType::Int32 => Ok(array.as_primitive::<Int32Type>().value(row) as f64),
        DataType::Int64 => Ok(array.as_primitive::<Int64Type>().value(row) as f64),
        DataType::Float32 => Ok(array.as_primitive::<Float32Type>().value(row) as f64),
        DataType::Float64 => Ok(array.as_primitive::<Float64Type>().value(row)),
        DataType::UInt64 => Ok(array.as_primitive::<UInt64Type>().value(row) as f64),
        dt => Err(PrismError::UnsupportedAggregation(format!("{:?}", dt))),
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
}
