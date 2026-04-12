//! Operator scaling benchmark — measures how operator performance scales
//! with data size from 100K to 10M rows. Validates O(n) / O(n log n)
//! scaling characteristics and identifies cache-boundary effects.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use prism_executor::filter_project::{evaluate_predicate, filter_batch, Predicate, ScalarValue};
use prism_executor::hash_aggregate::{hash_aggregate, AggExpr, AggFunc, HashAggConfig};
use prism_executor::hash_join::{hash_join, HashJoinConfig, JoinType};
use prism_executor::sort::{sort_batch_limit, NullOrdering, SortDirection, SortKey};

// ─── Data generators ─────────────────────────────────────────────────────────

fn make_fact(n: usize) -> RecordBatch {
    let flags = ["A", "N", "R"];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("dim_key", DataType::Int64, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("discount", DataType::Float64, false),
        Field::new("flag", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(
                (0..n).map(|i| (i % 50_000) as i64).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                (0..n).map(|i| (i % 100 + 1) as i32).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..n).map(|i| (i % 10000) as f64 * 0.01 + 1.0).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..n).map(|i| (i % 11) as f64 * 0.01).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| flags[i % 3]).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn make_dim(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("dim_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..n).map(|i| format!("dim_{:06}", i)).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..n).map(|i| (i as f64) * 3.14).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

// ─── Runner ──────────────────────────────────────────────────────────────────

fn bench_at_scale<F>(_name: &str, rows: usize, iters: usize, mut f: F) -> (f64, usize)
where
    F: FnMut() -> usize,
{
    for _ in 0..2 { f(); }
    let start = Instant::now();
    let mut out = 0;
    for _ in 0..iters { out = f(); }
    let per_iter = start.elapsed().as_secs_f64() / iters as f64;
    let ms = per_iter * 1000.0;
    let throughput = (rows as f64) / per_iter / 1_000_000.0;
    eprintln!(
        "  {:>12} rows: {:>10.2}ms  {:>8.2}M rows/s  → {} output",
        rows, ms, throughput, out
    );
    (ms, out)
}

// ─── Main ────────────────────────────────────────────────────────────────────

fn main() {
    eprintln!("╔══════════════════════════════════════════════════════════════════════════════╗");
    eprintln!("║          Prism Operator Scaling Benchmark (release build)                     ║");
    eprintln!("╚══════════════════════════════════════════════════════════════════════════════╝");
    eprintln!();

    let sizes = [100_000, 500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000];
    let dim_size = 50_000; // constant dimension table

    // ─── Filter (selectivity ~45%) ──────────────────────────────────────
    eprintln!("━━━ Filter (predicate: quantity < 45) — expected O(n) ━━━");
    let pred = Predicate::Lt(2, ScalarValue::Int32(45));
    let mut prev_ms = 0.0;
    for &n in &sizes {
        let iters = if n >= 5_000_000 { 3 } else { 10 };
        let fact = make_fact(n);
        let (ms, _) = bench_at_scale("filter", n, iters, || {
            let mask = evaluate_predicate(&fact, &pred).unwrap();
            filter_batch(&fact, &mask).unwrap().num_rows()
        });
        if prev_ms > 0.0 {
            eprint!("    scaling factor: {:.2}x (ideal 1.0x per proportional increase)", ms / prev_ms);
            eprintln!();
        }
        prev_ms = ms;
    }
    eprintln!();

    // ─── Hash Aggregate (GROUP BY flag — 3 groups) ──────────────────────
    eprintln!("━━━ Hash Aggregate (3 groups) — expected O(n) ━━━");
    let agg_config = HashAggConfig {
        group_by: vec![5],
        aggregates: vec![
            AggExpr { column: 3, func: AggFunc::Sum, output_name: "sum_amt".into() },
            AggExpr { column: 3, func: AggFunc::Avg, output_name: "avg_amt".into() },
            AggExpr { column: 0, func: AggFunc::Count, output_name: "cnt".into() },
        ],
    };
    let mut prev_ms = 0.0;
    for &n in &sizes {
        let iters = if n >= 5_000_000 { 3 } else { 10 };
        let fact = make_fact(n);
        let (ms, _) = bench_at_scale("hash_agg_low", n, iters, || {
            hash_aggregate(&fact, &agg_config).unwrap().num_rows()
        });
        if prev_ms > 0.0 {
            eprint!("    scaling factor: {:.2}x", ms / prev_ms);
            eprintln!();
        }
        prev_ms = ms;
    }
    eprintln!();

    // ─── Hash Aggregate (high-cardinality — 50K groups) ─────────────────
    eprintln!("━━━ Hash Aggregate (50K groups) — expected O(n), more hash table stress ━━━");
    let agg_high = HashAggConfig {
        group_by: vec![1], // dim_key: 50K distinct values
        aggregates: vec![
            AggExpr { column: 3, func: AggFunc::Sum, output_name: "sum_amt".into() },
            AggExpr { column: 0, func: AggFunc::Count, output_name: "cnt".into() },
        ],
    };
    let mut prev_ms = 0.0;
    for &n in &sizes {
        let iters = if n >= 5_000_000 { 3 } else { 10 };
        let fact = make_fact(n);
        let (ms, _) = bench_at_scale("hash_agg_high", n, iters, || {
            hash_aggregate(&fact, &agg_high).unwrap().num_rows()
        });
        if prev_ms > 0.0 {
            eprint!("    scaling factor: {:.2}x", ms / prev_ms);
            eprintln!();
        }
        prev_ms = ms;
    }
    eprintln!();

    // ─── Hash Join (fact × dim) ─────────────────────────────────────────
    eprintln!("━━━ Hash Join (fact × 50K dim) — expected O(n) probe, O(dim) build ━━━");
    let dim = make_dim(dim_size);
    let join_config = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![1],
        build_keys: vec![0],
    };
    let mut prev_ms = 0.0;
    for &n in &sizes {
        let iters = if n >= 5_000_000 { 3 } else { 5 };
        let fact = make_fact(n);
        let (ms, _) = bench_at_scale("hash_join", n, iters, || {
            hash_join(&fact, &dim, &join_config).unwrap().num_rows()
        });
        if prev_ms > 0.0 {
            eprint!("    scaling factor: {:.2}x", ms / prev_ms);
            eprintln!();
        }
        prev_ms = ms;
    }
    eprintln!();

    // ─── Sort (O(n log n)) ──────────────────────────────────────────────
    eprintln!("━━━ Sort (by amount DESC) — expected O(n log n) ━━━");
    let sort_keys = vec![SortKey {
        column: 3,
        direction: SortDirection::Desc,
        nulls: NullOrdering::NullsLast,
    }];
    let mut prev_ms = 0.0;
    // Skip 10M for sort — too slow for benchmarking
    for &n in &[100_000, 500_000, 1_000_000, 2_000_000, 5_000_000] {
        let iters = if n >= 5_000_000 { 2 } else { 5 };
        let fact = make_fact(n);
        let (ms, _) = bench_at_scale("sort", n, iters, || {
            sort_batch_limit(&fact, &sort_keys, fact.num_rows()).unwrap().num_rows()
        });
        if prev_ms > 0.0 {
            let ratio = n as f64 / (n as f64 / 2.0); // ideally compare n*log(n)
            eprint!("    scaling factor: {:.2}x (O(n log n) expected ~{:.2}x)", ms / prev_ms,
                ratio * (n as f64).ln() / ((n as f64 / 2.0).ln() * ratio));
            eprintln!();
        }
        prev_ms = ms;
    }
    eprintln!();

    // ─── Sort + Limit (should be much faster than full sort) ────────────
    eprintln!("━━━ Sort + LIMIT 100 — expected close to O(n) ━━━");
    let mut prev_ms = 0.0;
    for &n in &sizes {
        let iters = if n >= 5_000_000 { 3 } else { 10 };
        let fact = make_fact(n);
        let (ms, _) = bench_at_scale("sort_limit", n, iters, || {
            sort_batch_limit(&fact, &sort_keys, 100).unwrap().num_rows()
        });
        if prev_ms > 0.0 {
            eprint!("    scaling factor: {:.2}x", ms / prev_ms);
            eprintln!();
        }
        prev_ms = ms;
    }
    eprintln!();

    // ─── Compound pipeline: filter → join → agg → sort ─────────────────
    eprintln!("━━━ Full Pipeline: filter → join → agg → sort+limit ━━━");
    let pred = Predicate::Lt(4, ScalarValue::Float64(0.05));
    let agg_config = HashAggConfig {
        group_by: vec![5],
        aggregates: vec![
            AggExpr { column: 3, func: AggFunc::Sum, output_name: "sum_amt".into() },
        ],
    };
    let sort_keys = vec![SortKey {
        column: 1,
        direction: SortDirection::Desc,
        nulls: NullOrdering::NullsLast,
    }];

    let mut prev_ms = 0.0;
    for &n in &sizes {
        let iters = if n >= 5_000_000 { 2 } else { 5 };
        let fact = make_fact(n);
        let dim = make_dim(dim_size);

        let (ms, _) = bench_at_scale("full_pipeline", n, iters, || {
            let mask = evaluate_predicate(&fact, &pred).unwrap();
            let filtered = filter_batch(&fact, &mask).unwrap();
            let joined = hash_join(&filtered, &dim, &join_config).unwrap();
            let agg = hash_aggregate(&joined, &agg_config).unwrap();
            sort_batch_limit(&agg, &sort_keys, 10).unwrap().num_rows()
        });
        if prev_ms > 0.0 {
            eprint!("    scaling factor: {:.2}x", ms / prev_ms);
            eprintln!();
        }
        prev_ms = ms;
    }
    eprintln!();

    eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    eprintln!("Scaling characteristics:");
    eprintln!("  Filter, Agg, Join probe: O(n) — throughput should remain constant");
    eprintln!("  Sort (full):             O(n log n) — throughput decreases slightly");
    eprintln!("  Sort + LIMIT:            ~O(n) — partial sort with heap");
    eprintln!("  Cache effects expected at ~2M rows (L3 boundary on typical hardware)");
}
