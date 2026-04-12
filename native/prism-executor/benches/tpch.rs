//! TPC-H-like benchmark suite for Prism native operators.
//!
//! Simulates the compute-heavy portions of TPC-H queries to measure
//! raw operator throughput. This benchmarks the native execution engine
//! (Arrow SIMD operators) — not the full query lifecycle.
//!
//! Compare these numbers with Trino's per-operator timing on equivalent
//! data sizes to estimate the speedup from native execution.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use prism_executor::filter_project::{evaluate_predicate, filter_batch, Predicate, ScalarValue};
use prism_executor::hash_aggregate::{hash_aggregate, AggExpr, AggFunc, HashAggConfig};
use prism_executor::hash_join::{hash_join, HashJoinConfig, JoinType};
use prism_executor::sort::{sort_batch_limit, NullOrdering, SortDirection, SortKey};
use prism_executor::string_ops::{string_like, string_upper};

// ─── Data generators (TPC-H-like schemas) ─────────────────────────────────────

fn make_lineitem(n: usize) -> RecordBatch {
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

fn make_orders(n: usize) -> RecordBatch {
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

// ─── Benchmark runner ─────────────────────────────────────────────────────────

struct BenchResult {
    name: String,
    rows: usize,
    elapsed_ms: f64,
    throughput_mrows_sec: f64,
    output_rows: usize,
}

impl BenchResult {
    fn print(&self) {
        eprintln!(
            "  {:40} {:>8} rows  {:>8.2}ms  {:>8.2}M rows/s  → {} output rows",
            self.name, self.rows, self.elapsed_ms, self.throughput_mrows_sec, self.output_rows
        );
    }
}

fn bench_op<F>(name: &str, rows: usize, iterations: usize, mut f: F) -> BenchResult
where
    F: FnMut() -> usize,
{
    // Warmup
    for _ in 0..2 {
        f();
    }

    let start = Instant::now();
    let mut output_rows = 0;
    for _ in 0..iterations {
        output_rows = f();
    }
    let total = start.elapsed();
    let per_iter = total.as_secs_f64() / iterations as f64;

    BenchResult {
        name: name.to_string(),
        rows,
        elapsed_ms: per_iter * 1000.0,
        throughput_mrows_sec: (rows as f64) / per_iter / 1_000_000.0,
        output_rows,
    }
}

// ─── TPC-H Q1: Pricing Summary Report ────────────────────────────────────────
// SELECT l_returnflag, l_linestatus,
//        SUM(l_quantity), SUM(l_extendedprice),
//        AVG(l_discount), COUNT(*)
// FROM lineitem
// WHERE l_shipdate <= date '1998-09-02'  (simulated as quantity filter)
// GROUP BY l_returnflag, l_linestatus
// ORDER BY l_returnflag, l_linestatus

fn bench_q1(lineitem: &RecordBatch, label: &str) -> Vec<BenchResult> {
    let n = lineitem.num_rows();
    let iters = if n >= 1_000_000 { 5 } else { 20 };

    let mut results = Vec::new();

    // Filter: l_quantity < 40 (simulates date filter selecting ~80%)
    let pred = Predicate::Lt(4, ScalarValue::Float64(40.0));
    let r = bench_op(&format!("Q1 filter [{}]", label), n, iters, || {
        let mask = evaluate_predicate(lineitem, &pred).unwrap();
        let filtered = filter_batch(lineitem, &mask).unwrap();
        filtered.num_rows()
    });
    results.push(r);

    // Aggregate by (returnflag, linestatus) = columns 8, 9
    let mask = evaluate_predicate(lineitem, &pred).unwrap();
    let filtered = filter_batch(lineitem, &mask).unwrap();

    let agg_config = HashAggConfig {
        group_by: vec![8, 9],
        aggregates: vec![
            AggExpr { column: 4, func: AggFunc::Sum, output_name: "sum_qty".into() },
            AggExpr { column: 5, func: AggFunc::Sum, output_name: "sum_price".into() },
            AggExpr { column: 6, func: AggFunc::Avg, output_name: "avg_disc".into() },
            AggExpr { column: 0, func: AggFunc::Count, output_name: "count_order".into() },
        ],
    };
    let r = bench_op(
        &format!("Q1 hash_agg [{}]", label),
        filtered.num_rows(),
        iters,
        || {
            let result = hash_aggregate(&filtered, &agg_config).unwrap();
            result.num_rows()
        },
    );
    results.push(r);

    results
}

// ─── TPC-H Q3: Shipping Priority ─────────────────────────────────────────────
// SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue
// FROM customer, orders, lineitem
// WHERE c_mktsegment = 'BUILDING' AND o_orderdate < ...
// GROUP BY l_orderkey ORDER BY revenue DESC LIMIT 10

fn bench_q3(lineitem: &RecordBatch, orders: &RecordBatch, label: &str) -> Vec<BenchResult> {
    let iters = if lineitem.num_rows() >= 1_000_000 { 3 } else { 10 };

    let mut results = Vec::new();

    // Filter orders: orderstatus = 'F'
    let pred = Predicate::Eq(2, ScalarValue::Utf8("F".into()));
    let mask = evaluate_predicate(orders, &pred).unwrap();
    let filtered_orders = filter_batch(orders, &mask).unwrap();

    let r = bench_op(
        &format!("Q3 filter orders [{}]", label),
        orders.num_rows(),
        iters,
        || {
            let mask = evaluate_predicate(orders, &pred).unwrap();
            let f = filter_batch(orders, &mask).unwrap();
            f.num_rows()
        },
    );
    results.push(r);

    // Hash join: lineitem.l_orderkey = filtered_orders.o_orderkey
    let join_config = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![0],
        build_keys: vec![0],
    };
    let r = bench_op(
        &format!("Q3 hash_join [{}]", label),
        lineitem.num_rows(),
        iters,
        || {
            let joined = hash_join(lineitem, &filtered_orders, &join_config).unwrap();
            joined.num_rows()
        },
    );
    results.push(r);

    // Aggregate + sort
    let joined = hash_join(lineitem, &filtered_orders, &join_config).unwrap();
    let agg_config = HashAggConfig {
        group_by: vec![0],
        aggregates: vec![AggExpr {
            column: 5,
            func: AggFunc::Sum,
            output_name: "revenue".into(),
        }],
    };
    let r = bench_op(
        &format!("Q3 hash_agg [{}]", label),
        joined.num_rows(),
        iters,
        || {
            let agg = hash_aggregate(&joined, &agg_config).unwrap();
            agg.num_rows()
        },
    );
    results.push(r);

    let agg_result = hash_aggregate(&joined, &agg_config).unwrap();
    let sort_keys = vec![SortKey {
        column: 1,
        direction: SortDirection::Desc,
        nulls: NullOrdering::NullsLast,
    }];
    let r = bench_op(
        &format!("Q3 sort+limit10 [{}]", label),
        agg_result.num_rows(),
        iters,
        || {
            let sorted = sort_batch_limit(&agg_result, &sort_keys, 10).unwrap();
            sorted.num_rows()
        },
    );
    results.push(r);

    results
}

// ─── TPC-H Q6: Forecasting Revenue Change ────────────────────────────────────
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24

fn bench_q6(lineitem: &RecordBatch, label: &str) -> Vec<BenchResult> {
    let n = lineitem.num_rows();
    let iters = if n >= 1_000_000 { 10 } else { 30 };

    let mut results = Vec::new();

    // Compound filter
    let pred = Predicate::And(
        Box::new(Predicate::Lt(4, ScalarValue::Float64(24.0))),
        Box::new(Predicate::And(
            Box::new(Predicate::Ge(6, ScalarValue::Float64(0.05))),
            Box::new(Predicate::Le(6, ScalarValue::Float64(0.07))),
        )),
    );
    let r = bench_op(&format!("Q6 compound filter [{}]", label), n, iters, || {
        let mask = evaluate_predicate(lineitem, &pred).unwrap();
        let filtered = filter_batch(lineitem, &mask).unwrap();
        filtered.num_rows()
    });
    results.push(r);

    // Global aggregate
    let mask = evaluate_predicate(lineitem, &pred).unwrap();
    let filtered = filter_batch(lineitem, &mask).unwrap();
    let agg_config = HashAggConfig {
        group_by: vec![],
        aggregates: vec![AggExpr {
            column: 5,
            func: AggFunc::Sum,
            output_name: "revenue".into(),
        }],
    };
    let r = bench_op(
        &format!("Q6 global agg [{}]", label),
        filtered.num_rows(),
        iters,
        || {
            let result = hash_aggregate(&filtered, &agg_config).unwrap();
            result.num_rows()
        },
    );
    results.push(r);

    results
}

// ─── String operations benchmark ──────────────────────────────────────────────

fn bench_string_ops(lineitem: &RecordBatch, label: &str) -> Vec<BenchResult> {
    let n = lineitem.num_rows();
    let iters = if n >= 1_000_000 { 5 } else { 20 };
    let col = lineitem
        .column(8)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let mut results = Vec::new();

    let r = bench_op(&format!("LIKE filter [{}]", label), n, iters, || {
        let mask = string_like(col, "%A%").unwrap();
        mask.iter().filter(|v| v == &Some(true)).count()
    });
    results.push(r);

    let r = bench_op(&format!("UPPER [{}]", label), n, iters, || {
        let result = string_upper(col).unwrap();
        result.len()
    });
    results.push(r);

    results
}

// ─── Main benchmark entry ─────────────────────────────────────────────────────

fn main() {
    eprintln!("╔══════════════════════════════════════════════════════════════════════════╗");
    eprintln!("║          Prism TPC-H Operator Benchmark (release build)                 ║");
    eprintln!("╚══════════════════════════════════════════════════════════════════════════╝");
    eprintln!();

    for &(li_size, ord_size, label) in &[
        (100_000, 25_000, "SF~0.01"),
        (600_000, 150_000, "SF~0.1"),
        (6_000_000, 1_500_000, "SF~1"),
    ] {
        eprintln!("━━━ {} (lineitem={}, orders={}) ━━━", label, li_size, ord_size);

        let t0 = Instant::now();
        let lineitem = make_lineitem(li_size);
        let orders = make_orders(ord_size);
        eprintln!("  Data generation: {:.0}ms", t0.elapsed().as_millis());
        eprintln!();

        eprintln!("  TPC-H Q1 (Pricing Summary — filter + group-by agg):");
        for r in bench_q1(&lineitem, label) {
            r.print();
        }
        eprintln!();

        eprintln!("  TPC-H Q3 (Shipping Priority — filter + join + agg + sort):");
        for r in bench_q3(&lineitem, &orders, label) {
            r.print();
        }
        eprintln!();

        eprintln!("  TPC-H Q6 (Revenue Forecast — compound filter + global agg):");
        for r in bench_q6(&lineitem, label) {
            r.print();
        }
        eprintln!();

        eprintln!("  String Operations (LIKE + UPPER):");
        for r in bench_string_ops(&lineitem, label) {
            r.print();
        }
        eprintln!();
    }

    eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    eprintln!("Trino Baseline Reference (JVM, single-threaded operator, SF1):");
    eprintln!("  Q1 filter+agg:    ~800-1200ms  (JVM hash table + per-row boxing)");
    eprintln!("  Q3 join+agg:      ~2000-4000ms (DefaultPagesHash + serialization)");
    eprintln!("  Q6 filter+agg:    ~400-800ms   (ScanFilterAndProjectOperator)");
    eprintln!("  String LIKE:      ~500-1000ms  (VariableWidthBlock byte scan)");
    eprintln!();
    eprintln!("Note: Trino baselines are per-operator, single-thread estimates from");
    eprintln!("published benchmarks. Actual Trino wall-clock includes coordinator,");
    eprintln!("planning, serialization, and REST-based exchange overhead.");
}
