//! Sort operator on Arrow RecordBatches.
//!
//! Replaces Trino's `OrderByOperator`. Uses Arrow's built-in sort kernels.

use arrow::compute::{self, SortColumn, SortOptions};
use arrow_array::{Array, Float64Array, RecordBatch, UInt32Array};
use arrow_schema::DataType;
use rayon::prelude::*;

use crate::Result;

/// Sort direction for a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Null ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrdering {
    NullsFirst,
    NullsLast,
}

/// Specification for one sort key.
#[derive(Debug, Clone)]
pub struct SortKey {
    pub column: usize,
    pub direction: SortDirection,
    pub nulls: NullOrdering,
}

/// Sort a RecordBatch by multiple keys.
pub fn sort_batch(batch: &RecordBatch, sort_keys: &[SortKey]) -> Result<RecordBatch> {
    if batch.num_rows() == 0 || sort_keys.is_empty() {
        return Ok(batch.clone());
    }

    let sort_columns: Vec<SortColumn> = sort_keys
        .iter()
        .map(|key| SortColumn {
            values: batch.column(key.column).clone(),
            options: Some(SortOptions {
                descending: key.direction == SortDirection::Desc,
                nulls_first: key.nulls == NullOrdering::NullsFirst,
            }),
        })
        .collect();

    let indices = compute::lexsort_to_indices(&sort_columns, None)?;
    take_by_indices(batch, &indices)
}

/// Sort a RecordBatch and return only the first `limit` rows (Top-N).
pub fn sort_batch_limit(
    batch: &RecordBatch,
    sort_keys: &[SortKey],
    limit: usize,
) -> Result<RecordBatch> {
    if batch.num_rows() == 0 || sort_keys.is_empty() {
        return Ok(batch.clone());
    }

    let sort_columns: Vec<SortColumn> = sort_keys
        .iter()
        .map(|key| SortColumn {
            values: batch.column(key.column).clone(),
            options: Some(SortOptions {
                descending: key.direction == SortDirection::Desc,
                nulls_first: key.nulls == NullOrdering::NullsFirst,
            }),
        })
        .collect();

    let indices = compute::lexsort_to_indices(&sort_columns, Some(limit))?;
    take_by_indices(batch, &indices)
}

/// Merge two sorted RecordBatches into a single sorted batch.
pub fn merge_sorted(
    left: &RecordBatch,
    right: &RecordBatch,
    sort_keys: &[SortKey],
) -> Result<RecordBatch> {
    let merged = compute::concat_batches(&left.schema(), &[left.clone(), right.clone()])?;
    sort_batch(&merged, sort_keys)
}

/// Heap-based Top-N for a single sort key over many batches. Avoids Arrow's
/// sort kernel by maintaining a fixed-size min-heap (scanned via argmin) while
/// streaming over primitive values. Much faster than per-batch `sort_batch_limit`
/// + merge when limit is small.
///
/// Currently specialized for Float64 descending (the common TopN benchmark case).
/// Returns Ok(None) if not specialized — caller should fall back.
pub fn topn_single_col(
    batches: &[RecordBatch],
    sort_key: &SortKey,
    limit: usize,
) -> Result<Option<RecordBatch>> {
    if batches.is_empty() || limit == 0 {
        return Ok(None);
    }
    let schema = batches[0].schema();
    let col_idx = sort_key.column;
    let first_col = batches[0].column(col_idx);
    let is_desc = sort_key.direction == SortDirection::Desc;

    // Only specialized for Float64 DESC with NULLS LAST for now.
    if first_col.data_type() != &DataType::Float64 || !is_desc {
        return Ok(None);
    }

    // Phase 1 — per-batch parallel heap scan
    let per_batch: Vec<Vec<(f64, u32, u32)>> = batches
        .par_iter()
        .enumerate()
        .map(|(bi, batch)| scan_topn_f64_desc(batch, col_idx, limit, bi as u32))
        .collect();

    // Phase 2 — merge into global top-K
    let mut global: Vec<(f64, u32, u32)> = per_batch.into_iter().flatten().collect();
    global.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    global.truncate(limit);

    if global.is_empty() {
        return Ok(Some(RecordBatch::new_empty(schema)));
    }

    // Phase 3 — materialize rows in global order (zero-copy slice + concat)
    let slices: Vec<RecordBatch> = global
        .iter()
        .map(|(_, bi, ri)| batches[*bi as usize].slice(*ri as usize, 1))
        .collect();
    let result = compute::concat_batches(&schema, &slices)?;
    Ok(Some(result))
}

fn scan_topn_f64_desc(
    batch: &RecordBatch,
    col_idx: usize,
    limit: usize,
    batch_idx: u32,
) -> Vec<(f64, u32, u32)> {
    let arr = batch
        .column(col_idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("topn: expected Float64Array");
    let n = arr.len();
    if n == 0 {
        return Vec::new();
    }
    let k = limit.min(n);
    let mut top_vals: Vec<f64> = vec![f64::NEG_INFINITY; k];
    let mut top_rows: Vec<u32> = vec![0u32; k];
    let mut filled: usize = 0;
    let mut min_pos: usize = 0;
    let mut min_val: f64 = f64::NEG_INFINITY;
    let has_nulls = arr.null_count() > 0;
    let values: &[f64] = arr.values();

    if !has_nulls {
        for i in 0..n {
            let v = unsafe { *values.get_unchecked(i) };
            if filled < k {
                top_vals[filled] = v;
                top_rows[filled] = i as u32;
                filled += 1;
                if filled == k {
                    let (mp, mv) = argmin(&top_vals);
                    min_pos = mp;
                    min_val = mv;
                }
            } else if v > min_val {
                top_vals[min_pos] = v;
                top_rows[min_pos] = i as u32;
                let (mp, mv) = argmin(&top_vals);
                min_pos = mp;
                min_val = mv;
            }
        }
    } else {
        for i in 0..n {
            if arr.is_null(i) {
                continue;
            }
            let v = values[i];
            if filled < k {
                top_vals[filled] = v;
                top_rows[filled] = i as u32;
                filled += 1;
                if filled == k {
                    let (mp, mv) = argmin(&top_vals);
                    min_pos = mp;
                    min_val = mv;
                }
            } else if v > min_val {
                top_vals[min_pos] = v;
                top_rows[min_pos] = i as u32;
                let (mp, mv) = argmin(&top_vals);
                min_pos = mp;
                min_val = mv;
            }
        }
    }

    (0..filled)
        .map(|i| (top_vals[i], batch_idx, top_rows[i]))
        .collect()
}

#[inline(always)]
fn argmin(vals: &[f64]) -> (usize, f64) {
    let mut mp = 0;
    let mut mv = vals[0];
    for j in 1..vals.len() {
        if vals[j] < mv {
            mv = vals[j];
            mp = j;
        }
    }
    (mp, mv)
}

fn take_by_indices(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch> {
    let columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| compute::take(col, indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Charlie", "Alice", "Bob", "Dave", "Eve"])),
                Arc::new(Int64Array::from(vec![30, 25, 25, 35, 28])),
                Arc::new(Float64Array::from(vec![85.5, 92.0, 88.0, 76.0, 92.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_sort_by_name_asc() {
        let keys = vec![SortKey {
            column: 0,
            direction: SortDirection::Asc,
            nulls: NullOrdering::NullsLast,
        }];
        let result = sort_batch(&test_batch(), &keys).unwrap();
        let names = result.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(4), "Eve");
    }

    #[test]
    fn test_sort_by_score_desc_limit() {
        let keys = vec![SortKey {
            column: 2,
            direction: SortDirection::Desc,
            nulls: NullOrdering::NullsLast,
        }];
        let result = sort_batch_limit(&test_batch(), &keys, 3).unwrap();
        assert_eq!(result.num_rows(), 3);
        let scores = result.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(scores.value(0), 92.0);
    }

    #[test]
    fn test_multi_key_sort() {
        let keys = vec![
            SortKey { column: 1, direction: SortDirection::Asc, nulls: NullOrdering::NullsLast },
            SortKey { column: 2, direction: SortDirection::Desc, nulls: NullOrdering::NullsLast },
        ];
        let result = sort_batch(&test_batch(), &keys).unwrap();
        let ages = result.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let scores = result.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(ages.value(0), 25);
        assert_eq!(scores.value(0), 92.0);
    }
}
