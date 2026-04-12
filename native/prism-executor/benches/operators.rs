use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use prism_executor::filter_project::{evaluate_predicate, filter_batch, Predicate, ScalarValue};
use prism_executor::hash_aggregate::{hash_aggregate, AggExpr, AggFunc, HashAggConfig};
use prism_executor::hash_join::{hash_join, HashJoinConfig, JoinType};
use prism_executor::sort::{sort_batch, NullOrdering, SortDirection, SortKey};

fn make_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("group_key", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let ids: Vec<i64> = (0..n as i64).collect();
    let keys: Vec<i64> = (0..n as i64).map(|i| i % 100).collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(keys)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

fn bench_filter(c: &mut Criterion) {
    let batch = make_batch(100_000);
    let pred = Predicate::Gt(2, ScalarValue::Float64(50_000.0));

    c.bench_function("filter_100k_rows", |b| {
        b.iter(|| {
            let mask = evaluate_predicate(black_box(&batch), &pred).unwrap();
            let _ = filter_batch(black_box(&batch), &mask).unwrap();
        })
    });
}

fn bench_hash_aggregate(c: &mut Criterion) {
    let batch = make_batch(100_000);
    let config = HashAggConfig {
        group_by: vec![1],
        aggregates: vec![
            AggExpr {
                column: 2,
                func: AggFunc::Sum,
                output_name: "sum_value".into(),
            },
            AggExpr {
                column: 2,
                func: AggFunc::Count,
                output_name: "cnt".into(),
            },
        ],
    };

    c.bench_function("hash_agg_100k_rows_100_groups", |b| {
        b.iter(|| {
            let _ = hash_aggregate(black_box(&batch), &config).unwrap();
        })
    });
}

fn bench_hash_join(c: &mut Criterion) {
    let probe = make_batch(100_000);
    let build = make_batch(10_000);
    let config = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![1],
        build_keys: vec![1],
    };

    c.bench_function("hash_join_100k_probe_10k_build", |b| {
        b.iter(|| {
            let _ = hash_join(black_box(&probe), black_box(&build), &config).unwrap();
        })
    });
}

fn bench_sort(c: &mut Criterion) {
    let batch = make_batch(100_000);
    let keys = vec![SortKey {
        column: 2,
        direction: SortDirection::Desc,
        nulls: NullOrdering::NullsLast,
    }];

    c.bench_function("sort_100k_rows_desc", |b| {
        b.iter(|| {
            let _ = sort_batch(black_box(&batch), &keys).unwrap();
        })
    });
}

fn bench_filter_1m(c: &mut Criterion) {
    let batch = make_batch(1_000_000);
    let pred = Predicate::Gt(2, ScalarValue::Float64(500_000.0));

    c.bench_function("filter_1m_rows", |b| {
        b.iter(|| {
            let mask = evaluate_predicate(black_box(&batch), &pred).unwrap();
            let _ = filter_batch(black_box(&batch), &mask).unwrap();
        })
    });
}

fn bench_hash_aggregate_1m(c: &mut Criterion) {
    let batch = make_batch(1_000_000);
    let config = HashAggConfig {
        group_by: vec![1],
        aggregates: vec![
            AggExpr {
                column: 2,
                func: AggFunc::Sum,
                output_name: "sum_value".into(),
            },
            AggExpr {
                column: 2,
                func: AggFunc::Count,
                output_name: "cnt".into(),
            },
        ],
    };

    c.bench_function("hash_agg_1m_rows_100_groups", |b| {
        b.iter(|| {
            let _ = hash_aggregate(black_box(&batch), &config).unwrap();
        })
    });
}

fn bench_hash_join_1m(c: &mut Criterion) {
    let probe = make_batch(1_000_000);
    let build = make_batch(100_000);
    let config = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![1],
        build_keys: vec![1],
    };

    c.bench_function("hash_join_1m_probe_100k_build", |b| {
        b.iter(|| {
            let _ = hash_join(black_box(&probe), black_box(&build), &config).unwrap();
        })
    });
}

fn bench_sort_1m(c: &mut Criterion) {
    let batch = make_batch(1_000_000);
    let keys = vec![SortKey {
        column: 2,
        direction: SortDirection::Desc,
        nulls: NullOrdering::NullsLast,
    }];

    c.bench_function("sort_1m_rows_desc", |b| {
        b.iter(|| {
            let _ = sort_batch(black_box(&batch), &keys).unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_filter,
    bench_hash_aggregate,
    bench_hash_join,
    bench_sort,
    bench_filter_1m,
    bench_hash_aggregate_1m,
    bench_hash_join_1m,
    bench_sort_1m,
);
criterion_main!(benches);
