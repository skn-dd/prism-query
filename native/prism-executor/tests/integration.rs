//! End-to-end integration tests simulating TPC-H-like query pipelines.
//!
//! Tests realistic data sizes (100K–1M rows) across the full operator stack:
//! scan → filter → project → hash join → hash aggregate → sort.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use prism_executor::filter_project::{
    evaluate_predicate, filter_and_project, filter_batch, Predicate, ScalarValue,
};
use prism_executor::hash_aggregate::{hash_aggregate, AggExpr, AggFunc, HashAggConfig};
use prism_executor::hash_join::{hash_join, HashJoinConfig, JoinType};
use prism_executor::sort::{sort_batch, sort_batch_limit, NullOrdering, SortDirection, SortKey};
use prism_executor::string_ops::{string_like, string_upper};

// ─── Data generators ──────────────────────────────────────────────────────────

fn make_lineitem(n: usize) -> RecordBatch {
    let regions = ["EAST", "WEST", "NORTH", "SOUTH", "CENTRAL"];
    let schema = Arc::new(Schema::new(vec![
        Field::new("orderkey", DataType::Int64, false),
        Field::new("partkey", DataType::Int64, false),
        Field::new("quantity", DataType::Float64, false),
        Field::new("extendedprice", DataType::Float64, false),
        Field::new("discount", DataType::Float64, false),
        Field::new("tax", DataType::Float64, false),
        Field::new("shipregion", DataType::Utf8, false),
    ]));

    let orderkeys: Vec<i64> = (0..n as i64).collect();
    let partkeys: Vec<i64> = (0..n as i64).map(|i| i % 2000).collect();
    let quantities: Vec<f64> = (0..n).map(|i| (i % 50) as f64 + 1.0).collect();
    let prices: Vec<f64> = (0..n).map(|i| ((i % 10000) as f64) * 1.23 + 10.0).collect();
    let discounts: Vec<f64> = (0..n).map(|i| (i % 10) as f64 * 0.01).collect();
    let taxes: Vec<f64> = (0..n).map(|i| (i % 8) as f64 * 0.01).collect();
    let ship_regions: Vec<&str> = (0..n).map(|i| regions[i % 5]).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(orderkeys)),
            Arc::new(Int64Array::from(partkeys)),
            Arc::new(Float64Array::from(quantities)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Float64Array::from(discounts)),
            Arc::new(Float64Array::from(taxes)),
            Arc::new(StringArray::from(ship_regions)),
        ],
    )
    .unwrap()
}

fn make_orders(n: usize) -> RecordBatch {
    let statuses = ["F", "O", "P"];
    let schema = Arc::new(Schema::new(vec![
        Field::new("orderkey", DataType::Int64, false),
        Field::new("custkey", DataType::Int64, false),
        Field::new("orderstatus", DataType::Utf8, false),
        Field::new("totalprice", DataType::Float64, false),
    ]));

    let orderkeys: Vec<i64> = (0..n as i64).collect();
    let custkeys: Vec<i64> = (0..n as i64).map(|i| i % 15000).collect();
    let status_vals: Vec<&str> = (0..n).map(|i| statuses[i % 3]).collect();
    let prices: Vec<f64> = (0..n).map(|i| (i as f64) * 2.5 + 100.0).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(orderkeys)),
            Arc::new(Int64Array::from(custkeys)),
            Arc::new(StringArray::from(status_vals)),
            Arc::new(Float64Array::from(prices)),
        ],
    )
    .unwrap()
}

// ─── TPC-H Q6-like: scan → filter → aggregate ────────────────────────────────
// SELECT SUM(extendedprice * discount) AS revenue
// FROM lineitem
// WHERE quantity < 24 AND discount BETWEEN 0.05 AND 0.07

#[test]
fn test_tpch_q6_like_100k() {
    run_tpch_q6(100_000, "100K");
}

#[test]
fn test_tpch_q6_like_1m() {
    run_tpch_q6(1_000_000, "1M");
}

fn run_tpch_q6(n: usize, label: &str) {
    let t0 = Instant::now();
    let lineitem = make_lineitem(n);
    let gen_ms = t0.elapsed().as_millis();

    // Filter: quantity < 24
    let t1 = Instant::now();
    let pred = Predicate::Lt(2, ScalarValue::Float64(24.0));
    let mask = evaluate_predicate(&lineitem, &pred).unwrap();
    let filtered = filter_batch(&lineitem, &mask).unwrap();
    let filter_ms = t1.elapsed().as_millis();

    assert!(filtered.num_rows() < n, "filter should reduce rows");
    assert!(filtered.num_rows() > 0, "filter should keep some rows");

    // Aggregate: SUM(extendedprice), COUNT(*)
    let t2 = Instant::now();
    let agg_config = HashAggConfig {
        group_by: vec![], // global aggregate
        aggregates: vec![
            AggExpr {
                column: 3, // extendedprice
                func: AggFunc::Sum,
                output_name: "revenue".into(),
            },
            AggExpr {
                column: 0,
                func: AggFunc::Count,
                output_name: "cnt".into(),
            },
        ],
    };
    let result = hash_aggregate(&filtered, &agg_config).unwrap();
    let agg_ms = t2.elapsed().as_millis();

    assert_eq!(result.num_rows(), 1, "global agg should produce 1 row");

    eprintln!(
        "[Q6 {}] gen={}ms filter={}ms({} → {} rows) agg={}ms total={}ms",
        label,
        gen_ms,
        filter_ms,
        n,
        filtered.num_rows(),
        agg_ms,
        t0.elapsed().as_millis()
    );
}

// ─── TPC-H Q1-like: scan → filter → group-by aggregate → sort ────────────────
// SELECT shipregion, SUM(quantity), SUM(extendedprice), AVG(discount), COUNT(*)
// FROM lineitem
// GROUP BY shipregion
// ORDER BY shipregion

#[test]
fn test_tpch_q1_like_100k() {
    run_tpch_q1(100_000, "100K");
}

#[test]
fn test_tpch_q1_like_1m() {
    run_tpch_q1(1_000_000, "1M");
}

fn run_tpch_q1(n: usize, label: &str) {
    let t0 = Instant::now();
    let lineitem = make_lineitem(n);

    // Aggregate by shipregion (column 6)
    let t1 = Instant::now();
    let agg_config = HashAggConfig {
        group_by: vec![6], // shipregion
        aggregates: vec![
            AggExpr {
                column: 2,
                func: AggFunc::Sum,
                output_name: "sum_qty".into(),
            },
            AggExpr {
                column: 3,
                func: AggFunc::Sum,
                output_name: "sum_price".into(),
            },
            AggExpr {
                column: 4,
                func: AggFunc::Avg,
                output_name: "avg_disc".into(),
            },
            AggExpr {
                column: 0,
                func: AggFunc::Count,
                output_name: "count_order".into(),
            },
        ],
    };
    let agg_result = hash_aggregate(&lineitem, &agg_config).unwrap();
    let agg_ms = t1.elapsed().as_millis();

    assert_eq!(agg_result.num_rows(), 5, "5 regions");

    // Sort by group key (column 0 of agg output = shipregion)
    let t2 = Instant::now();
    let sort_keys = vec![SortKey {
        column: 0,
        direction: SortDirection::Asc,
        nulls: NullOrdering::NullsLast,
    }];
    let sorted = sort_batch(&agg_result, &sort_keys).unwrap();
    let sort_ms = t2.elapsed().as_millis();

    assert_eq!(sorted.num_rows(), 5);

    eprintln!(
        "[Q1 {}] gen={}ms agg={}ms sort={}ms total={}ms",
        label,
        t0.elapsed().as_millis() - agg_ms as u128 - sort_ms as u128,
        agg_ms,
        sort_ms,
        t0.elapsed().as_millis()
    );
}

// ─── TPC-H Q3-like: join → filter → aggregate → sort-limit ───────────────────
// SELECT orderkey, SUM(extendedprice * (1-discount)) AS revenue
// FROM orders JOIN lineitem ON orders.orderkey = lineitem.orderkey
// WHERE orderstatus = 'F'
// GROUP BY orderkey
// ORDER BY revenue DESC
// LIMIT 10

#[test]
fn test_tpch_q3_like_100k() {
    run_tpch_q3(100_000, 50_000, "100K");
}

#[test]
fn test_tpch_q3_like_500k() {
    run_tpch_q3(500_000, 200_000, "500K");
}

fn run_tpch_q3(lineitem_rows: usize, orders_rows: usize, label: &str) {
    let t0 = Instant::now();
    let lineitem = make_lineitem(lineitem_rows);
    let orders = make_orders(orders_rows);

    // Filter orders: orderstatus = 'F' (column 2)
    let t1 = Instant::now();
    let pred = Predicate::Eq(2, ScalarValue::Utf8("F".into()));
    let mask = evaluate_predicate(&orders, &pred).unwrap();
    let filtered_orders = filter_batch(&orders, &mask).unwrap();
    let filter_ms = t1.elapsed().as_millis();

    // Join: filtered_orders.orderkey (col 0) = lineitem.orderkey (col 0)
    let t2 = Instant::now();
    let join_config = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![0], // lineitem.orderkey
        build_keys: vec![0], // filtered_orders.orderkey
    };
    let joined = hash_join(&lineitem, &filtered_orders, &join_config).unwrap();
    let join_ms = t2.elapsed().as_millis();

    assert!(joined.num_rows() > 0, "join should produce results");

    // Aggregate by orderkey (col 0 of lineitem side of join)
    let t3 = Instant::now();
    let agg_config = HashAggConfig {
        group_by: vec![0],
        aggregates: vec![AggExpr {
            column: 3, // extendedprice from lineitem
            func: AggFunc::Sum,
            output_name: "revenue".into(),
        }],
    };
    let agg_result = hash_aggregate(&joined, &agg_config).unwrap();
    let agg_ms = t3.elapsed().as_millis();

    // Sort by revenue DESC, limit 10
    let t4 = Instant::now();
    let sort_keys = vec![SortKey {
        column: 1,
        direction: SortDirection::Desc,
        nulls: NullOrdering::NullsLast,
    }];
    let top10 = sort_batch_limit(&agg_result, &sort_keys, 10).unwrap();
    let sort_ms = t4.elapsed().as_millis();

    assert!(top10.num_rows() <= 10);

    eprintln!(
        "[Q3 {}] filter={}ms join={}ms({} rows) agg={}ms sort={}ms total={}ms",
        label,
        filter_ms,
        join_ms,
        joined.num_rows(),
        agg_ms,
        sort_ms,
        t0.elapsed().as_millis()
    );
}

// ─── String operations pipeline ───────────────────────────────────────────────

#[test]
fn test_string_pipeline_100k() {
    let n = 100_000;
    let regions: Vec<&str> = (0..n).map(|i| match i % 5 {
        0 => "east_coast",
        1 => "west_coast",
        2 => "central",
        3 => "north_west",
        _ => "south_east",
    }).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(regions)),
        ],
    )
    .unwrap();

    let t0 = Instant::now();

    // LIKE filter
    let region_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    let like_mask = string_like(region_col, "%coast%").unwrap();
    let coast_count = like_mask.iter().filter(|v| v == &Some(true)).count();
    assert_eq!(coast_count, 40_000); // 2/5 of rows

    // UPPER
    let upper_result = string_upper(region_col).unwrap();
    assert_eq!(upper_result.value(0), "EAST_COAST");

    let elapsed = t0.elapsed().as_millis();
    eprintln!("[String ops 100K] LIKE+UPPER={}ms", elapsed);
}

// ─── Multi-stage pipeline (filter → project → agg → sort) ────────────────────

#[test]
fn test_full_pipeline_100k() {
    let n = 100_000;
    let lineitem = make_lineitem(n);

    let t0 = Instant::now();

    // 1. Filter + project
    let pred = Predicate::Gt(2, ScalarValue::Float64(25.0)); // quantity > 25
    let columns = vec![0, 1, 2, 3, 6]; // orderkey, partkey, quantity, extendedprice, shipregion
    let projected = filter_and_project(&lineitem, Some(&pred), &columns).unwrap();
    assert!(projected.num_rows() < n);

    // 2. Aggregate by shipregion (now column 4 after projection)
    let agg_config = HashAggConfig {
        group_by: vec![4],
        aggregates: vec![
            AggExpr {
                column: 3,
                func: AggFunc::Sum,
                output_name: "total_price".into(),
            },
            AggExpr {
                column: 2,
                func: AggFunc::Avg,
                output_name: "avg_qty".into(),
            },
            AggExpr {
                column: 0,
                func: AggFunc::Count,
                output_name: "order_count".into(),
            },
        ],
    };
    let agg_result = hash_aggregate(&projected, &agg_config).unwrap();
    assert_eq!(agg_result.num_rows(), 5);

    // 3. Sort by total_price DESC
    let sort_keys = vec![SortKey {
        column: 1,
        direction: SortDirection::Desc,
        nulls: NullOrdering::NullsLast,
    }];
    let sorted = sort_batch(&agg_result, &sort_keys).unwrap();
    assert_eq!(sorted.num_rows(), 5);

    eprintln!("[Full pipeline 100K] total={}ms", t0.elapsed().as_millis());
}
