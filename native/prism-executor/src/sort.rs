//! Sort operator on Arrow RecordBatches.
//!
//! Replaces Trino's `OrderByOperator`. Uses Arrow's built-in sort kernels.

use arrow::compute::{self, SortColumn, SortOptions};
use arrow_array::{RecordBatch, UInt32Array};

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
