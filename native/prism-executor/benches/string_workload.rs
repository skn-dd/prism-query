//! String-heavy workload benchmark — focuses on SIMD string operations
//! that are typically slow in the JVM due to per-character processing
//! on VariableWidthBlock.
//!
//! Patterns: LIKE, ILIKE, CONTAINS, STARTS_WITH, UPPER, LOWER,
//! SUBSTRING, CONCAT, REPLACE, LENGTH — at various data sizes
//! and string lengths.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use prism_executor::string_ops::*;
use prism_executor::filter_project::filter_batch;
use prism_executor::hash_aggregate::{hash_aggregate, AggExpr, AggFunc, HashAggConfig};

// ─── Data generators ─────────────────────────────────────────────────────────

/// Short strings (5-15 chars) — typical for codes, flags, IDs
fn make_short_strings(n: usize) -> StringArray {
    let patterns = [
        "AAAA", "BXYZ", "CCMP", "DNET", "EFGH",
        "alfa", "bravo", "charlie", "delta", "echo",
        "FOX01", "GOL02", "HIJ03", "KLM04", "NOP05",
    ];
    StringArray::from(
        (0..n).map(|i| format!("{}-{:05}", patterns[i % patterns.len()], i % 99999)).collect::<Vec<_>>(),
    )
}

/// Medium strings (30-80 chars) — typical for names, descriptions
fn make_medium_strings(n: usize) -> StringArray {
    let first = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael"];
    let last = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"];
    let city = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
        "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    ];
    StringArray::from(
        (0..n)
            .map(|i| {
                format!(
                    "{} {} from {} (customer #{:06})",
                    first[i % first.len()],
                    last[i % last.len()],
                    city[i % city.len()],
                    i
                )
            })
            .collect::<Vec<_>>(),
    )
}

/// Long strings (100-300 chars) — typical for descriptions, comments, emails
fn make_long_strings(n: usize) -> StringArray {
    let templates = [
        "This is a product description for item {} in the {} category with extended warranty and free shipping included in the base price",
        "Customer feedback report #{}: The {} service was exceptional and exceeded all expectations. Would highly recommend to friends and family members",
        "Order confirmation #{} for {} department. Your order has been processed and will be shipped within 3-5 business days via standard delivery",
        "Meeting notes from session {}: Discussed {} project timeline, budget allocation, resource planning, and quarterly deliverables for next review",
    ];
    let categories = ["electronics", "clothing", "home", "sports", "automotive"];
    StringArray::from(
        (0..n)
            .map(|i| {
                format!(
                    "{}",
                    templates[i % templates.len()]
                        .replace("{}", &i.to_string())
                        .replace("{}", categories[i % categories.len()])
                )
            })
            .collect::<Vec<_>>(),
    )
}

// ─── Benchmark runner ────────────────────────────────────────────────────────

struct BenchResult {
    name: String,
    rows: usize,
    elapsed_ms: f64,
    throughput_mrows_sec: f64,
    extra: String,
}

impl BenchResult {
    fn print(&self) {
        eprintln!(
            "  {:55} {:>8} rows  {:>8.2}ms  {:>8.2}M rows/s  {}",
            self.name, self.rows, self.elapsed_ms, self.throughput_mrows_sec, self.extra
        );
    }
}

fn bench_op<F>(name: &str, rows: usize, iters: usize, extra: &str, mut f: F) -> BenchResult
where
    F: FnMut() -> usize,
{
    for _ in 0..2 { f(); }
    let start = Instant::now();
    let mut _out = 0;
    for _ in 0..iters { _out = f(); }
    let per_iter = start.elapsed().as_secs_f64() / iters as f64;

    BenchResult {
        name: name.to_string(),
        rows,
        elapsed_ms: per_iter * 1000.0,
        throughput_mrows_sec: (rows as f64) / per_iter / 1_000_000.0,
        extra: extra.to_string(),
    }
}

// ─── LIKE pattern matching ───────────────────────────────────────────────────

fn bench_like(arr: &StringArray, label: &str) -> Vec<BenchResult> {
    let n = arr.len();
    let iters = if n >= 1_000_000 { 5 } else { 20 };
    let mut results = Vec::new();

    // Prefix LIKE (uses starts_with optimization)
    results.push(bench_op(
        &format!("LIKE 'A%' (prefix) [{}]", label), n, iters, "",
        || string_like(arr, "A%").unwrap().iter().filter(|v| v == &Some(true)).count(),
    ));

    // Suffix LIKE
    results.push(bench_op(
        &format!("LIKE '%05' (suffix) [{}]", label), n, iters, "",
        || string_like(arr, "%05").unwrap().iter().filter(|v| v == &Some(true)).count(),
    ));

    // Contains LIKE (most expensive — full scan)
    results.push(bench_op(
        &format!("LIKE '%bravo%' (contains) [{}]", label), n, iters, "",
        || string_like(arr, "%bravo%").unwrap().iter().filter(|v| v == &Some(true)).count(),
    ));

    // Complex LIKE with multiple wildcards
    results.push(bench_op(
        &format!("LIKE '%a%o%' (multi-wild) [{}]", label), n, iters, "",
        || string_like(arr, "%a%o%").unwrap().iter().filter(|v| v == &Some(true)).count(),
    ));

    results
}

// ─── String transformations ──────────────────────────────────────────────────

fn bench_transforms(arr: &StringArray, label: &str) -> Vec<BenchResult> {
    let n = arr.len();
    let iters = if n >= 1_000_000 { 5 } else { 20 };
    let mut results = Vec::new();

    results.push(bench_op(
        &format!("UPPER [{}]", label), n, iters, "",
        || string_upper(arr).unwrap().len(),
    ));

    results.push(bench_op(
        &format!("LOWER [{}]", label), n, iters, "",
        || string_lower(arr).unwrap().len(),
    ));

    results.push(bench_op(
        &format!("SUBSTRING(1,10) [{}]", label), n, iters, "",
        || string_substring(arr, 1, Some(10)).unwrap().len(),
    ));

    results.push(bench_op(
        &format!("LENGTH [{}]", label), n, iters, "",
        || { let r = string_length(arr).unwrap(); r.len() },
    ));

    results.push(bench_op(
        &format!("REPLACE('-','_') [{}]", label), n, iters, "",
        || string_replace(arr, "-", "_").unwrap().len(),
    ));

    results
}

// ─── String search operations ────────────────────────────────────────────────

fn bench_search(arr: &StringArray, label: &str) -> Vec<BenchResult> {
    let n = arr.len();
    let iters = if n >= 1_000_000 { 5 } else { 20 };
    let mut results = Vec::new();

    results.push(bench_op(
        &format!("CONTAINS('bravo') [{}]", label), n, iters, "",
        || string_contains(arr, "bravo").unwrap().iter().filter(|v| v == &Some(true)).count(),
    ));

    results.push(bench_op(
        &format!("STARTS_WITH('AA') [{}]", label), n, iters, "",
        || string_starts_with(arr, "AA").unwrap().iter().filter(|v| v == &Some(true)).count(),
    ));

    results.push(bench_op(
        &format!("ENDS_WITH('05') [{}]", label), n, iters, "",
        || string_ends_with(arr, "05").unwrap().iter().filter(|v| v == &Some(true)).count(),
    ));

    results
}

// ─── String CONCAT (element-wise) ───────────────────────────────────────────

fn bench_concat(n: usize, label: &str) -> Vec<BenchResult> {
    let iters = if n >= 1_000_000 { 3 } else { 10 };
    let left = make_short_strings(n);
    let right = make_short_strings(n);
    let mut results = Vec::new();

    results.push(bench_op(
        &format!("CONCAT(a, b) [{}]", label), n, iters, "",
        || string_concat(&left, &right).unwrap().len(),
    ));

    results
}

// ─── String-driven aggregation pipeline ──────────────────────────────────────
// Simulates: SELECT UPPER(flag), COUNT(*), SUM(amount)
// FROM table WHERE flag LIKE '%A%' GROUP BY UPPER(flag)

fn bench_string_pipeline(n: usize, label: &str) -> Vec<BenchResult> {
    let iters = if n >= 1_000_000 { 3 } else { 10 };
    let mut results = Vec::new();

    let categories = [
        "Electronics", "Music", "Books", "Sports", "Home", "Jewelry",
        "Women", "Men", "Children", "Shoes", "Automotive", "Garden",
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::Int64Array::from((0..n as i64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..n).map(|i| categories[i % categories.len()]).collect::<Vec<_>>(),
            )),
            Arc::new(arrow_array::Float64Array::from(
                (0..n).map(|i| (i as f64) * 0.01 + 5.0).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    // Filter with string LIKE
    let cat_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    results.push(bench_op(
        &format!("Pipeline: LIKE '%o%' filter [{}]", label), n, iters, "",
        || {
            let mask = string_like(cat_col, "%o%").unwrap();
            filter_batch(&batch, &mask).unwrap().num_rows()
        },
    ));

    let like_mask = string_like(cat_col, "%o%").unwrap();
    let filtered = filter_batch(&batch, &like_mask).unwrap();

    // UPPER transform on filtered result
    let filtered_cat = filtered.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    results.push(bench_op(
        &format!("Pipeline: UPPER(category) [{}]", label), filtered.num_rows(), iters, "",
        || string_upper(filtered_cat).unwrap().len(),
    ));

    // Aggregate by category
    let agg_config = HashAggConfig {
        group_by: vec![1],
        aggregates: vec![
            AggExpr { column: 2, func: AggFunc::Sum, output_name: "total".into() },
            AggExpr { column: 0, func: AggFunc::Count, output_name: "cnt".into() },
        ],
    };
    results.push(bench_op(
        &format!("Pipeline: GROUP BY category [{}]", label), filtered.num_rows(), iters, "",
        || hash_aggregate(&filtered, &agg_config).unwrap().num_rows(),
    ));

    results
}

// ─── Main ────────────────────────────────────────────────────────────────────

fn main() {
    eprintln!("╔══════════════════════════════════════════════════════════════════════════════╗");
    eprintln!("║         Prism String-Heavy Workload Benchmark (release build)                 ║");
    eprintln!("╚══════════════════════════════════════════════════════════════════════════════╝");
    eprintln!();

    for &(n, label) in &[
        (100_000, "100K"),
        (500_000, "500K"),
        (2_000_000, "2M"),
    ] {
        eprintln!("━━━ {} rows ━━━", label);
        eprintln!();

        // Short strings
        eprintln!("  Short strings (~10 chars, codes/IDs):");
        let short = make_short_strings(n);
        eprintln!("  LIKE patterns:");
        for r in bench_like(&short, &format!("short/{}", label)) { r.print(); }
        eprintln!("  Transforms:");
        for r in bench_transforms(&short, &format!("short/{}", label)) { r.print(); }
        eprintln!("  Search:");
        for r in bench_search(&short, &format!("short/{}", label)) { r.print(); }
        eprintln!();

        // Medium strings
        eprintln!("  Medium strings (~50 chars, names/addresses):");
        let medium = make_medium_strings(n);
        eprintln!("  LIKE patterns:");
        for r in bench_like(&medium, &format!("med/{}", label)) { r.print(); }
        eprintln!("  Transforms:");
        for r in bench_transforms(&medium, &format!("med/{}", label)) { r.print(); }
        eprintln!();

        // Long strings
        eprintln!("  Long strings (~150 chars, descriptions):");
        let long = make_long_strings(n);
        eprintln!("  LIKE patterns:");
        for r in bench_like(&long, &format!("long/{}", label)) { r.print(); }
        eprintln!("  Transforms:");
        for r in bench_transforms(&long, &format!("long/{}", label)) { r.print(); }
        eprintln!();

        // CONCAT
        eprintln!("  Concatenation:");
        for r in bench_concat(n, label) { r.print(); }
        eprintln!();

        // Full pipeline
        eprintln!("  String-driven aggregation pipeline (LIKE→UPPER→GROUP BY):");
        for r in bench_string_pipeline(n, label) { r.print(); }
        eprintln!();
    }

    eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    eprintln!("Trino String Baseline (JVM, single-thread, 1M rows):");
    eprintln!("  LIKE prefix:            ~200-400ms   (optimized path in Trino)");
    eprintln!("  LIKE contains:          ~500-1000ms  (VariableWidthBlock byte scan)");
    eprintln!("  UPPER/LOWER:            ~300-600ms   (per-char Character.toUpperCase)");
    eprintln!("  SUBSTRING:              ~200-400ms   (Slice offset computation)");
    eprintln!("  CONCAT:                 ~400-800ms   (allocate + copy per row)");
    eprintln!();
    eprintln!("Prism advantages on strings:");
    eprintln!("  • Arrow string kernels use SIMD (SSE4.2/AVX2) for byte scanning");
    eprintln!("  • Contiguous UTF-8 buffer (not per-row Slice objects)");
    eprintln!("  • No JVM object header overhead per string");
    eprintln!("  • LIKE prefix/suffix patterns use specialized kernel paths");
}
