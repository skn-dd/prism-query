//! Concurrent operator throughput benchmark — measures multi-threaded
//! execution of operators to simulate a worker processing multiple
//! pipeline tasks simultaneously.
//!
//! Tests: independent parallel operators, pipelined (producer/consumer),
//! and mixed workloads across threads.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use prism_executor::filter_project::{evaluate_predicate, filter_batch, Predicate, ScalarValue};
use prism_executor::hash_aggregate::{hash_aggregate, AggExpr, AggFunc, HashAggConfig};
use prism_executor::hash_join::{hash_join, HashJoinConfig, JoinType};
use prism_executor::sort::{sort_batch_limit, NullOrdering, SortDirection, SortKey};
use prism_executor::string_ops::string_like;

// ─── Data generators ─────────────────────────────────────────────────────────

fn make_batch(n: usize) -> RecordBatch {
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

// ─── Benchmark helpers ───────────────────────────────────────────────────────

fn run_filter(batch: &RecordBatch) -> usize {
    let pred = Predicate::Lt(2, ScalarValue::Int32(45));
    let mask = evaluate_predicate(batch, &pred).unwrap();
    filter_batch(batch, &mask).unwrap().num_rows()
}

fn run_aggregate(batch: &RecordBatch) -> usize {
    let config = HashAggConfig {
        group_by: vec![5],
        aggregates: vec![
            AggExpr { column: 3, func: AggFunc::Sum, output_name: "sum".into() },
            AggExpr { column: 0, func: AggFunc::Count, output_name: "cnt".into() },
        ],
    };
    hash_aggregate(batch, &config).unwrap().num_rows()
}

fn run_join(probe: &RecordBatch, build: &RecordBatch) -> usize {
    let config = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![1],
        build_keys: vec![0],
    };
    hash_join(probe, build, &config).unwrap().num_rows()
}

fn run_sort(batch: &RecordBatch) -> usize {
    let keys = vec![SortKey {
        column: 3,
        direction: SortDirection::Desc,
        nulls: NullOrdering::NullsLast,
    }];
    sort_batch_limit(batch, &keys, 100).unwrap().num_rows()
}

fn run_string_filter(batch: &RecordBatch) -> usize {
    let col = batch.column(5).as_any().downcast_ref::<StringArray>().unwrap();
    let mask = string_like(col, "%A%").unwrap();
    filter_batch(batch, &mask).unwrap().num_rows()
}

fn run_full_pipeline(batch: &RecordBatch, dim: &RecordBatch) -> usize {
    let pred = Predicate::Lt(4, ScalarValue::Float64(0.05));
    let mask = evaluate_predicate(batch, &pred).unwrap();
    let filtered = filter_batch(batch, &mask).unwrap();

    let join_config = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![1],
        build_keys: vec![0],
    };
    let joined = hash_join(&filtered, dim, &join_config).unwrap();

    let agg_config = HashAggConfig {
        group_by: vec![5],
        aggregates: vec![
            AggExpr { column: 3, func: AggFunc::Sum, output_name: "sum".into() },
        ],
    };
    hash_aggregate(&joined, &agg_config).unwrap().num_rows()
}

// ─── Concurrent benchmarks ───────────────────────────────────────────────────

fn bench_parallel_same_op(n: usize, num_threads: usize) -> f64 {
    let batches: Vec<_> = (0..num_threads).map(|_| make_batch(n)).collect();

    let start = Instant::now();
    std::thread::scope(|s| {
        let handles: Vec<_> = batches
            .iter()
            .map(|batch| s.spawn(|| run_filter(batch)))
            .collect();
        for h in handles { h.join().unwrap(); }
    });
    start.elapsed().as_secs_f64() * 1000.0
}

fn bench_parallel_mixed_ops(n: usize, num_threads: usize) -> f64 {
    let batches: Vec<_> = (0..num_threads).map(|_| make_batch(n)).collect();
    let dim = make_dim(50_000);

    let start = Instant::now();
    std::thread::scope(|s| {
        let handles: Vec<_> = batches
            .iter()
            .enumerate()
            .map(|(i, batch)| {
                let dim = &dim;
                s.spawn(move || match i % 5 {
                    0 => run_filter(batch),
                    1 => run_aggregate(batch),
                    2 => run_join(batch, dim),
                    3 => run_sort(batch),
                    _ => run_string_filter(batch),
                })
            })
            .collect();
        for h in handles { h.join().unwrap(); }
    });
    start.elapsed().as_secs_f64() * 1000.0
}

fn bench_parallel_pipelines(n: usize, num_threads: usize) -> f64 {
    let batches: Vec<_> = (0..num_threads).map(|_| make_batch(n)).collect();
    let dim = make_dim(50_000);

    let start = Instant::now();
    std::thread::scope(|s| {
        let handles: Vec<_> = batches
            .iter()
            .map(|batch| {
                let dim = &dim;
                s.spawn(move || run_full_pipeline(batch, dim))
            })
            .collect();
        for h in handles { h.join().unwrap(); }
    });
    start.elapsed().as_secs_f64() * 1000.0
}

// ─── Main ────────────────────────────────────────────────────────────────────

fn main() {
    let cpus = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4);

    eprintln!("╔══════════════════════════════════════════════════════════════════════════════╗");
    eprintln!("║         Prism Concurrent Operator Throughput Benchmark                        ║");
    eprintln!("╚══════════════════════════════════════════════════════════════════════════════╝");
    eprintln!();
    eprintln!("  Available CPUs: {}", cpus);
    eprintln!();

    let n = 1_000_000;
    let thread_counts: Vec<usize> = vec![1, 2, 4, 8]
        .into_iter()
        .filter(|&t| t <= cpus * 2)
        .collect();

    // ─── Parallel filter (same operation on independent data) ────────
    eprintln!("━━━ Parallel Filter (1M rows each, independent batches) ━━━");
    let baseline = bench_parallel_same_op(n, 1);
    eprintln!("  {:>2} threads: {:>10.2}ms  (baseline)", 1, baseline);
    for &threads in &thread_counts {
        if threads == 1 { continue; }
        let ms = bench_parallel_same_op(n, threads);
        let speedup = (baseline * threads as f64) / ms;
        let efficiency = speedup / threads as f64 * 100.0;
        eprintln!(
            "  {:>2} threads: {:>10.2}ms  speedup={:.2}x  efficiency={:.0}%  (total {} M rows)",
            threads, ms, speedup, efficiency, threads
        );
    }
    eprintln!();

    // ─── Parallel mixed operators ────────────────────────────────────
    eprintln!("━━━ Parallel Mixed Ops (filter/agg/join/sort/string, 1M rows each) ━━━");
    let baseline = bench_parallel_mixed_ops(n, 1);
    eprintln!("  {:>2} threads: {:>10.2}ms  (baseline — single mixed op)", 1, baseline);
    for &threads in &thread_counts {
        if threads < 2 { continue; }
        // Need at least 5 threads for one of each type
        let t = threads.max(5);
        let ms = bench_parallel_mixed_ops(n, t);
        let speedup = (baseline * t as f64) / ms;
        let efficiency = speedup / t as f64 * 100.0;
        eprintln!(
            "  {:>2} threads: {:>10.2}ms  speedup={:.2}x  efficiency={:.0}%",
            t, ms, speedup, efficiency
        );
    }
    eprintln!();

    // ─── Parallel full pipelines ─────────────────────────────────────
    eprintln!("━━━ Parallel Full Pipelines (filter→join→agg, 1M rows each) ━━━");
    let baseline = bench_parallel_pipelines(n, 1);
    eprintln!("  {:>2} threads: {:>10.2}ms  (baseline — single pipeline)", 1, baseline);
    for &threads in &thread_counts {
        if threads == 1 { continue; }
        let ms = bench_parallel_pipelines(n, threads);
        let speedup = (baseline * threads as f64) / ms;
        let efficiency = speedup / threads as f64 * 100.0;
        eprintln!(
            "  {:>2} threads: {:>10.2}ms  speedup={:.2}x  efficiency={:.0}%  ({} pipelines)",
            threads, ms, speedup, efficiency, threads
        );
    }
    eprintln!();

    // ─── Throughput scaling (fixed time, increasing threads) ─────────
    eprintln!("━━━ Throughput Scaling (total rows processed in 2s) ━━━");
    let batch_size = 500_000;
    for &threads in &thread_counts {
        let batch_per_thread = make_batch(batch_size);
        let dim = make_dim(50_000);
        let start = Instant::now();
        let mut total_rows = 0u64;

        while start.elapsed().as_secs_f64() < 2.0 {
            std::thread::scope(|s| {
                let handles: Vec<_> = (0..threads)
                    .map(|_| {
                        let b = &batch_per_thread;
                        let d = &dim;
                        s.spawn(move || run_full_pipeline(b, d))
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                    total_rows += batch_size as u64;
                }
            });
        }
        let elapsed = start.elapsed().as_secs_f64();
        let throughput = total_rows as f64 / elapsed / 1_000_000.0;
        eprintln!(
            "  {:>2} threads: {:>10.2}M rows/s  ({} total rows in {:.1}s)",
            threads, throughput, total_rows, elapsed
        );
    }
    eprintln!();

    eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    eprintln!("Key observations:");
    eprintln!("  • Arrow operators are embarrassingly parallel on independent data");
    eprintln!("  • No GIL, no synchronized hash table rehashing, no GC stop-the-world");
    eprintln!("  • Memory-bound ops (join, sort) may show lower efficiency at high thread counts");
    eprintln!("  • Trino JVM baseline: G1GC pauses cause latency spikes under concurrent load");
}
