//! Hash join operator on Arrow RecordBatches.
//!
//! Replaces Trino's `DefaultPagesHash` / `BigintPagesHash`. Builds a hash table
//! from the build side, then probes with the probe side.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::compute;
use arrow_array::{Array, RecordBatch, UInt32Array};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type, Float64Type};
use arrow_ord::cmp;
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{PrismError, Result};

/// Join type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    LeftSemi,
    LeftAnti,
}

/// Configuration for a hash join operation.
#[derive(Debug, Clone)]
pub struct HashJoinConfig {
    pub join_type: JoinType,
    pub build_keys: Vec<usize>,
    pub probe_keys: Vec<usize>,
}

/// A hash table built from the build side of a join.
pub struct HashTable {
    index: HashMap<u64, Vec<u32>>,
    build_batch: RecordBatch,
    key_cols: Vec<usize>,
}

impl HashTable {
    pub fn build(batch: &RecordBatch, key_cols: &[usize]) -> Result<Self> {
        let num_rows = batch.num_rows();
        let mut index: HashMap<u64, Vec<u32>> = HashMap::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            let h = hash_row(batch, key_cols, row_idx);
            index.entry(h).or_default().push(row_idx as u32);
        }

        Ok(HashTable {
            index,
            build_batch: batch.clone(),
            key_cols: key_cols.to_vec(),
        })
    }

    pub fn probe(
        &self,
        probe_batch: &RecordBatch,
        probe_key_cols: &[usize],
    ) -> Result<(UInt32Array, UInt32Array)> {
        let mut probe_indices = Vec::new();
        let mut build_indices = Vec::new();

        for probe_row in 0..probe_batch.num_rows() {
            let h = hash_row(probe_batch, probe_key_cols, probe_row);
            if let Some(build_rows) = self.index.get(&h) {
                for &build_row in build_rows {
                    if keys_equal(
                        probe_batch,
                        probe_key_cols,
                        probe_row,
                        &self.build_batch,
                        &self.key_cols,
                        build_row as usize,
                    )? {
                        probe_indices.push(probe_row as u32);
                        build_indices.push(build_row);
                    }
                }
            }
        }

        Ok((
            UInt32Array::from(probe_indices),
            UInt32Array::from(build_indices),
        ))
    }
}

/// Execute a hash join between probe (left) and build (right) RecordBatches.
pub fn hash_join(
    probe_batch: &RecordBatch,
    build_batch: &RecordBatch,
    config: &HashJoinConfig,
) -> Result<RecordBatch> {
    let hash_table = HashTable::build(build_batch, &config.build_keys)?;
    let (probe_idx, build_idx) = hash_table.probe(probe_batch, &config.probe_keys)?;

    match config.join_type {
        JoinType::Inner => {
            assemble_joined_batch(probe_batch, &probe_idx, build_batch, &build_idx)
        }
        JoinType::Left => {
            let (p_idx, b_idx) =
                left_join_indices(probe_batch.num_rows(), &probe_idx, &build_idx);
            assemble_joined_batch(probe_batch, &p_idx, build_batch, &b_idx)
        }
        JoinType::LeftSemi => {
            let matched: Vec<bool> = semi_join_mask(probe_batch.num_rows(), &probe_idx);
            let mask = arrow_array::BooleanArray::from(matched);
            let columns: Vec<_> = probe_batch
                .columns()
                .iter()
                .map(|c| compute::filter(c, &mask))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(RecordBatch::try_new(probe_batch.schema(), columns)?)
        }
        JoinType::LeftAnti => {
            let matched: Vec<bool> = semi_join_mask(probe_batch.num_rows(), &probe_idx)
                .into_iter()
                .map(|m| !m)
                .collect();
            let mask = arrow_array::BooleanArray::from(matched);
            let columns: Vec<_> = probe_batch
                .columns()
                .iter()
                .map(|c| compute::filter(c, &mask))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(RecordBatch::try_new(probe_batch.schema(), columns)?)
        }
        JoinType::Right => {
            // Right join: all build rows preserved, unmatched get nulls on probe side
            let (p_idx, b_idx) =
                right_join_indices(build_batch.num_rows(), &probe_idx, &build_idx);
            assemble_joined_batch_nullable_probe(probe_batch, &p_idx, build_batch, &b_idx)
        }
        JoinType::Full => {
            // Full outer join: all rows from both sides preserved
            let (p_idx, b_idx) =
                full_join_indices(probe_batch.num_rows(), build_batch.num_rows(), &probe_idx, &build_idx);
            assemble_joined_batch_both_nullable(probe_batch, &p_idx, build_batch, &b_idx)
        }
    }
}

// --- Internal helpers ---

/// Hash a single row's key columns.
fn hash_row(batch: &RecordBatch, key_cols: &[usize], row: usize) -> u64 {
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
        DataType::Int32 => {
            let arr = array.as_primitive::<Int32Type>();
            arr.value(row).hash(hasher);
        }
        DataType::Int64 => {
            let arr = array.as_primitive::<Int64Type>();
            arr.value(row).hash(hasher);
        }
        DataType::Float64 => {
            let arr = array.as_primitive::<Float64Type>();
            arr.value(row).to_bits().hash(hasher);
        }
        DataType::Utf8 => {
            let arr = array.as_string::<i32>();
            arr.value(row).hash(hasher);
        }
        _ => {
            // Fallback: hash the string representation
            format!("{:?}", array.slice(row, 1)).hash(hasher);
        }
    }
}

/// Assemble the joined output batch.
fn assemble_joined_batch(
    probe: &RecordBatch,
    probe_idx: &UInt32Array,
    build: &RecordBatch,
    build_idx: &UInt32Array,
) -> Result<RecordBatch> {
    let mut columns = Vec::new();
    let mut fields = Vec::new();

    for (i, field) in probe.schema().fields().iter().enumerate() {
        fields.push(field.clone());
        columns.push(compute::take(probe.column(i), probe_idx, None)?);
    }

    for (i, field) in build.schema().fields().iter().enumerate() {
        let mut f = field.as_ref().clone();
        if probe.schema().field_with_name(f.name()).is_ok() {
            f = Field::new(
                format!("{}_right", f.name()),
                f.data_type().clone(),
                f.is_nullable(),
            );
        }
        fields.push(Arc::new(f));
        columns.push(compute::take(build.column(i), build_idx, None)?);
    }

    let schema = SchemaRef::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn left_join_indices(
    probe_rows: usize,
    matched_probe: &UInt32Array,
    matched_build: &UInt32Array,
) -> (UInt32Array, UInt32Array) {
    let mut probe_out = Vec::new();
    let mut build_out = Vec::new();
    let mut matched_set = vec![false; probe_rows];

    for i in 0..matched_probe.len() {
        let p = matched_probe.value(i);
        let b = matched_build.value(i);
        matched_set[p as usize] = true;
        probe_out.push(p);
        build_out.push(b);
    }

    for row in 0..probe_rows {
        if !matched_set[row] {
            probe_out.push(row as u32);
            build_out.push(0); // Will produce nulls in the build side
        }
    }

    (
        UInt32Array::from(probe_out),
        UInt32Array::from(build_out),
    )
}

fn semi_join_mask(probe_rows: usize, matched_probe: &UInt32Array) -> Vec<bool> {
    let mut mask = vec![false; probe_rows];
    for i in 0..matched_probe.len() {
        mask[matched_probe.value(i) as usize] = true;
    }
    mask
}

/// For RIGHT JOIN: all build rows preserved; unmatched build rows get null probe indices.
/// Returns (probe_indices, build_indices) where probe_indices uses null for unmatched.
fn right_join_indices(
    build_rows: usize,
    matched_probe: &UInt32Array,
    matched_build: &UInt32Array,
) -> (UInt32Array, UInt32Array) {
    let mut probe_out = Vec::new();
    let mut build_out = Vec::new();
    let mut build_matched = vec![false; build_rows];

    // Include all matched pairs
    for i in 0..matched_probe.len() {
        probe_out.push(Some(matched_probe.value(i)));
        build_out.push(matched_build.value(i));
        build_matched[matched_build.value(i) as usize] = true;
    }

    // Include unmatched build rows with null probe
    for row in 0..build_rows {
        if !build_matched[row] {
            probe_out.push(None);
            build_out.push(row as u32);
        }
    }

    // For probe: use nullable UInt32Array
    let probe_arr: UInt32Array = probe_out.into_iter().collect();
    let build_arr = UInt32Array::from(build_out);
    (probe_arr, build_arr)
}

/// For FULL JOIN: all rows from both sides.
fn full_join_indices(
    probe_rows: usize,
    build_rows: usize,
    matched_probe: &UInt32Array,
    matched_build: &UInt32Array,
) -> (UInt32Array, UInt32Array) {
    let mut probe_out: Vec<Option<u32>> = Vec::new();
    let mut build_out: Vec<Option<u32>> = Vec::new();
    let mut probe_matched = vec![false; probe_rows];
    let mut build_matched = vec![false; build_rows];

    // Include all matched pairs
    for i in 0..matched_probe.len() {
        let p = matched_probe.value(i);
        let b = matched_build.value(i);
        probe_matched[p as usize] = true;
        build_matched[b as usize] = true;
        probe_out.push(Some(p));
        build_out.push(Some(b));
    }

    // Include unmatched probe rows (null on build side)
    for row in 0..probe_rows {
        if !probe_matched[row] {
            probe_out.push(Some(row as u32));
            build_out.push(None);
        }
    }

    // Include unmatched build rows (null on probe side)
    for row in 0..build_rows {
        if !build_matched[row] {
            probe_out.push(None);
            build_out.push(Some(row as u32));
        }
    }

    let probe_arr: UInt32Array = probe_out.into_iter().collect();
    let build_arr: UInt32Array = build_out.into_iter().collect();
    (probe_arr, build_arr)
}

/// Assemble joined batch where probe indices may be null (RIGHT JOIN).
fn assemble_joined_batch_nullable_probe(
    probe: &RecordBatch,
    probe_idx: &UInt32Array,
    build: &RecordBatch,
    build_idx: &UInt32Array,
) -> Result<RecordBatch> {
    let mut columns = Vec::new();
    let mut fields = Vec::new();

    // Probe side: use take which handles null indices -> null output values
    for (i, field) in probe.schema().fields().iter().enumerate() {
        let f = Field::new(field.name(), field.data_type().clone(), true); // nullable
        fields.push(Arc::new(f));
        columns.push(compute::take(probe.column(i), probe_idx, None)?);
    }

    for (i, field) in build.schema().fields().iter().enumerate() {
        let mut f = field.as_ref().clone();
        if probe.schema().field_with_name(f.name()).is_ok() {
            f = Field::new(format!("{}_right", f.name()), f.data_type().clone(), f.is_nullable());
        }
        fields.push(Arc::new(f));
        columns.push(compute::take(build.column(i), build_idx, None)?);
    }

    let schema = SchemaRef::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Assemble joined batch where both sides may have null indices (FULL JOIN).
fn assemble_joined_batch_both_nullable(
    probe: &RecordBatch,
    probe_idx: &UInt32Array,
    build: &RecordBatch,
    build_idx: &UInt32Array,
) -> Result<RecordBatch> {
    let mut columns = Vec::new();
    let mut fields = Vec::new();

    for (i, field) in probe.schema().fields().iter().enumerate() {
        let f = Field::new(field.name(), field.data_type().clone(), true);
        fields.push(Arc::new(f));
        columns.push(compute::take(probe.column(i), probe_idx, None)?);
    }

    for (i, field) in build.schema().fields().iter().enumerate() {
        let mut f = field.as_ref().clone();
        if probe.schema().field_with_name(f.name()).is_ok() {
            f = Field::new(format!("{}_right", f.name()), f.data_type().clone(), true);
        }
        fields.push(Arc::new(f));
        columns.push(compute::take(build.column(i), build_idx, None)?);
    }

    let schema = SchemaRef::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn keys_equal(
    batch_a: &RecordBatch,
    keys_a: &[usize],
    row_a: usize,
    batch_b: &RecordBatch,
    keys_b: &[usize],
    row_b: usize,
) -> Result<bool> {
    for (&ka, &kb) in keys_a.iter().zip(keys_b.iter()) {
        let col_a = batch_a.column(ka);
        let col_b = batch_b.column(kb);
        let val_a = col_a.slice(row_a, 1);
        let val_b = col_b.slice(row_b, 1);
        let eq_result = cmp::eq(&val_a, &val_b)?;
        if !eq_result.value(0) {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::Field;

    fn build_side() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dept_id", DataType::Int64, false),
            Field::new("dept_name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Engineering", "Sales", "Marketing"])),
            ],
        )
        .unwrap()
    }

    fn probe_side() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("emp_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("dept_id", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![100, 101, 102, 103])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol", "Dave"])),
                Arc::new(Int64Array::from(vec![1, 2, 1, 99])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_inner_join() {
        let config = HashJoinConfig {
            join_type: JoinType::Inner,
            probe_keys: vec![2],
            build_keys: vec![0],
        };
        let result = hash_join(&probe_side(), &build_side(), &config).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 5);
    }

    #[test]
    fn test_left_semi_join() {
        let config = HashJoinConfig {
            join_type: JoinType::LeftSemi,
            probe_keys: vec![2],
            build_keys: vec![0],
        };
        let result = hash_join(&probe_side(), &build_side(), &config).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 3);
    }

    #[test]
    fn test_left_anti_join() {
        let config = HashJoinConfig {
            join_type: JoinType::LeftAnti,
            probe_keys: vec![2],
            build_keys: vec![0],
        };
        let result = hash_join(&probe_side(), &build_side(), &config).unwrap();
        assert_eq!(result.num_rows(), 1);
    }
}
