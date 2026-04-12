//! TPC-H data generators with scale factor support.

use std::sync::Arc;

use arrow_array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

/// Generate TPC-H lineitem table.
/// SF1 = 6_000_000 rows, SF0.1 = 600_000 rows.
pub fn make_lineitem(sf: f64) -> RecordBatch {
    let n = (6_000_000.0 * sf) as usize;
    let schema = Arc::new(Schema::new(vec![
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
    ]));

    let flags = ["A", "N", "R"];
    let statuses = ["F", "O"];

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(
                (0..n).map(|i| (i % 200_000) as i64).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..n).map(|i| (i % 10_000) as i64).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                (0..n).map(|i| (i % 7 + 1) as i32).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..n).map(|i| (i % 50 + 1) as f64).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..n)
                    .map(|i| ((i % 100_000) as f64) * 0.99 + 900.0)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..n).map(|i| (i % 11) as f64 * 0.01).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..n).map(|i| (i % 9) as f64 * 0.01).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| flags[i % 3]).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| statuses[i % 2]).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// Generate TPC-H orders table.
/// SF1 = 1_500_000 rows, SF0.1 = 150_000 rows.
pub fn make_orders(sf: f64) -> RecordBatch {
    let n = (1_500_000.0 * sf) as usize;
    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
    ]));

    let statuses = ["F", "O", "P"];
    let priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(
                (0..n).map(|i| (i % 150_000) as i64).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| statuses[i % 3]).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..n)
                    .map(|i| (i as f64) * 2.5 + 100.0)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| priorities[i % 5]).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}
