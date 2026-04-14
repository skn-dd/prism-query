//! TPC-H data generators with scale factor support.

use std::sync::Arc;

use arrow_array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

/// Generate TPC-H lineitem table as a single RecordBatch.
/// SF1 = 6_000_000 rows, SF0.1 = 600_000 rows.
pub fn make_lineitem(sf: f64) -> RecordBatch {
    let batches = make_lineitem_chunked(sf, usize::MAX);
    let schema = batches[0].schema();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

/// Generate TPC-H lineitem table as chunked RecordBatches.
/// Each chunk has at most `chunk_size` rows (default 5M for SF100 compatibility).
pub fn make_lineitem_chunked(sf: f64, chunk_size: usize) -> Vec<RecordBatch> {
    let n = (6_000_000.0 * sf) as usize;
    make_lineitem_shard(n, 0, chunk_size)
}

/// Generate a shard of TPC-H lineitem table.
/// `row_count` rows starting from global `row_offset`, in chunks of `chunk_size`.
pub fn make_lineitem_shard(row_count: usize, row_offset: usize, chunk_size: usize) -> Vec<RecordBatch> {
    let schema = lineitem_schema();
    let flags = ["A", "N", "R"];
    let statuses = ["F", "O"];

    let mut batches = Vec::new();
    let mut local = 0usize;
    while local < row_count {
        let chunk_end = (local + chunk_size).min(row_count);
        let g_start = row_offset + local;
        let g_end = row_offset + chunk_end;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from((g_start..g_end).map(|i| i as i64).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(
                    (g_start..g_end).map(|i| (i % 200_000) as i64).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    (g_start..g_end).map(|i| (i % 10_000) as i64).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    (g_start..g_end).map(|i| (i % 7 + 1) as i32).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (g_start..g_end).map(|i| (i % 50 + 1) as f64).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (g_start..g_end)
                        .map(|i| ((i % 100_000) as f64) * 0.99 + 900.0)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (g_start..g_end).map(|i| (i % 11) as f64 * 0.01).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (g_start..g_end).map(|i| (i % 9) as f64 * 0.01).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (g_start..g_end).map(|i| flags[i % 3]).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (g_start..g_end).map(|i| statuses[i % 2]).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        batches.push(batch);
        local = chunk_end;
    }
    batches
}

pub fn lineitem_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
    ]))
}

/// Generate TPC-H orders table as a single RecordBatch.
/// SF1 = 1_500_000 rows, SF0.1 = 150_000 rows.
pub fn make_orders(sf: f64) -> RecordBatch {
    let batches = make_orders_chunked(sf, usize::MAX);
    let schema = batches[0].schema();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

/// Generate TPC-H orders table as chunked RecordBatches.
pub fn make_orders_chunked(sf: f64, chunk_size: usize) -> Vec<RecordBatch> {
    let n = (1_500_000.0 * sf) as usize;
    make_orders_shard(n, 0, chunk_size)
}

/// Generate a shard of TPC-H orders table.
/// `row_count` rows starting from global `row_offset`, in chunks of `chunk_size`.
pub fn make_orders_shard(row_count: usize, row_offset: usize, chunk_size: usize) -> Vec<RecordBatch> {
    let schema = orders_schema();
    let statuses = ["F", "O", "P"];
    let priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];

    let mut batches = Vec::new();
    let mut local = 0usize;
    while local < row_count {
        let chunk_end = (local + chunk_size).min(row_count);
        let g_start = row_offset + local;
        let g_end = row_offset + chunk_end;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from((g_start..g_end).map(|i| i as i64).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(
                    (g_start..g_end).map(|i| (i % 150_000) as i64).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (g_start..g_end).map(|i| statuses[i % 3]).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (g_start..g_end)
                        .map(|i| (i as f64) * 2.5 + 100.0)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (g_start..g_end).map(|i| priorities[i % 5]).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        batches.push(batch);
        local = chunk_end;
    }
    batches
}

pub fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
    ]))
}
