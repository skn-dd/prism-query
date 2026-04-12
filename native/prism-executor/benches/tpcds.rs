//! TPC-DS-like benchmark suite for Prism native operators.
//!
//! TPC-DS stresses different patterns than TPC-H:
//! - Star-schema joins (fact → multiple dimensions)
//! - Multi-way joins (3+ tables in a single query)
//! - High-cardinality GROUP BY (many distinct groups)
//! - Nested aggregation (aggregate, then re-aggregate)
//! - Heavy string matching and grouping on text columns
//! - Large dimension tables joined to enormous fact tables
//!
//! We simulate representative TPC-DS patterns using the existing
//! Prism operators: filter_project, hash_join, hash_aggregate, sort, string_ops.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use prism_executor::filter_project::{evaluate_predicate, filter_batch, Predicate, ScalarValue};
use prism_executor::hash_aggregate::{hash_aggregate, AggExpr, AggFunc, HashAggConfig};
use prism_executor::hash_join::{hash_join, HashJoinConfig, JoinType};
use prism_executor::sort::{sort_batch_limit, NullOrdering, SortDirection, SortKey};
use prism_executor::string_ops::{string_like, string_contains, string_upper, string_substring};

// ─── Data generators (TPC-DS-like schemas) ───────────────────────────────────

/// store_sales fact table — the core TPC-DS fact
fn make_store_sales(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ss_sold_date_sk", DataType::Int64, false),      // 0 — FK → date_dim
        Field::new("ss_item_sk", DataType::Int64, false),            // 1 — FK → item
        Field::new("ss_customer_sk", DataType::Int64, false),        // 2 — FK → customer
        Field::new("ss_store_sk", DataType::Int32, false),           // 3 — FK → store
        Field::new("ss_promo_sk", DataType::Int32, false),           // 4 — FK → promotion
        Field::new("ss_quantity", DataType::Int32, false),           // 5
        Field::new("ss_wholesale_cost", DataType::Float64, false),   // 6
        Field::new("ss_list_price", DataType::Float64, false),       // 7
        Field::new("ss_sales_price", DataType::Float64, false),      // 8
        Field::new("ss_ext_sales_price", DataType::Float64, false),  // 9
        Field::new("ss_net_profit", DataType::Float64, false),       // 10
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            // ss_sold_date_sk: date keys 2450816..2453005 (~6 years, TPC-DS date range)
            Arc::new(Int64Array::from(
                (0..n).map(|i| 2450816 + (i % 2190) as i64).collect::<Vec<_>>(),
            )),
            // ss_item_sk: 1..18000 items
            Arc::new(Int64Array::from(
                (0..n).map(|i| (i % 18_000 + 1) as i64).collect::<Vec<_>>(),
            )),
            // ss_customer_sk: 1..100000 customers
            Arc::new(Int64Array::from(
                (0..n).map(|i| (i % 100_000 + 1) as i64).collect::<Vec<_>>(),
            )),
            // ss_store_sk: 1..400 stores
            Arc::new(Int32Array::from(
                (0..n).map(|i| (i % 400 + 1) as i32).collect::<Vec<_>>(),
            )),
            // ss_promo_sk: 1..300 promos
            Arc::new(Int32Array::from(
                (0..n).map(|i| (i % 300 + 1) as i32).collect::<Vec<_>>(),
            )),
            // ss_quantity: 1..100
            Arc::new(Int32Array::from(
                (0..n).map(|i| (i % 100 + 1) as i32).collect::<Vec<_>>(),
            )),
            // ss_wholesale_cost: 1.00..100.00
            Arc::new(Float64Array::from(
                (0..n).map(|i| (i % 10000) as f64 * 0.01 + 1.0).collect::<Vec<_>>(),
            )),
            // ss_list_price: 1.00..300.00
            Arc::new(Float64Array::from(
                (0..n).map(|i| (i % 30000) as f64 * 0.01 + 1.0).collect::<Vec<_>>(),
            )),
            // ss_sales_price: 0.50..250.00
            Arc::new(Float64Array::from(
                (0..n).map(|i| (i % 25000) as f64 * 0.01 + 0.5).collect::<Vec<_>>(),
            )),
            // ss_ext_sales_price: quantity * sales_price
            Arc::new(Float64Array::from(
                (0..n)
                    .map(|i| {
                        let qty = (i % 100 + 1) as f64;
                        let price = (i % 25000) as f64 * 0.01 + 0.5;
                        qty * price
                    })
                    .collect::<Vec<_>>(),
            )),
            // ss_net_profit: ext_sales - wholesale
            Arc::new(Float64Array::from(
                (0..n)
                    .map(|i| {
                        let ext = (i % 100 + 1) as f64 * ((i % 25000) as f64 * 0.01 + 0.5);
                        let cost = (i % 10000) as f64 * 0.01 + 1.0;
                        ext - cost
                    })
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// date_dim dimension table
fn make_date_dim(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("d_date_sk", DataType::Int64, false),       // 0 — PK
        Field::new("d_year", DataType::Int32, false),           // 1
        Field::new("d_moy", DataType::Int32, false),            // 2 — month of year
        Field::new("d_dom", DataType::Int32, false),            // 3 — day of month
        Field::new("d_qoy", DataType::Int32, false),            // 4 — quarter of year
        Field::new("d_day_name", DataType::Utf8, false),        // 5
    ]));

    let days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                (0..n).map(|i| 2450816 + i as i64).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                (0..n).map(|i| 1998 + (i / 365) as i32).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                (0..n).map(|i| (i % 12 + 1) as i32).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                (0..n).map(|i| (i % 28 + 1) as i32).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                (0..n).map(|i| (i % 4 + 1) as i32).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| days[i % 7]).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// item dimension table — high cardinality text columns
fn make_item(n: usize) -> RecordBatch {
    let categories = [
        "Electronics", "Music", "Books", "Sports", "Home", "Jewelry",
        "Women", "Men", "Children", "Shoes",
    ];
    let classes = [
        "portable", "classical", "fiction", "outdoor", "furniture",
        "diamonds", "dresses", "shirts", "newborn", "athletic",
        "personal", "pop", "cooking", "camping", "bedding",
        "gold", "pants", "accessories", "infants", "boots",
    ];
    let brands = [
        "exportiunivamalg #1", "edu packscholar #2", "importoamalg #1",
        "amalgimporto #1", "scholaramalgamalg #14", "exportimaxi #1",
        "corpmaxi #6", "amalgamalg #1", "importoimporto #1", "scholarmaxi #4",
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("i_item_sk", DataType::Int64, false),         // 0 — PK
        Field::new("i_item_id", DataType::Utf8, false),          // 1
        Field::new("i_item_desc", DataType::Utf8, false),        // 2
        Field::new("i_category", DataType::Utf8, false),         // 3
        Field::new("i_class", DataType::Utf8, false),            // 4
        Field::new("i_brand", DataType::Utf8, false),            // 5
        Field::new("i_current_price", DataType::Float64, false), // 6
        Field::new("i_manager_id", DataType::Int32, false),      // 7
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                (0..n).map(|i| (i + 1) as i64).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n)
                    .map(|i| format!("AAAAAAAAA{}AAAA", i % 99999))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n)
                    .map(|i| format!("Item description for product {}", i))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| categories[i % categories.len()]).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| classes[i % classes.len()]).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| brands[i % brands.len()]).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..n).map(|i| (i % 500) as f64 + 0.99).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                (0..n).map(|i| (i % 100 + 1) as i32).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// customer dimension table
fn make_customer(n: usize) -> RecordBatch {
    let states = [
        "CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
        "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
    ];
    let _genders = ["M", "F"];

    let schema = Arc::new(Schema::new(vec![
        Field::new("c_customer_sk", DataType::Int64, false),        // 0 — PK
        Field::new("c_customer_id", DataType::Utf8, false),         // 1
        Field::new("c_first_name", DataType::Utf8, false),          // 2
        Field::new("c_last_name", DataType::Utf8, false),           // 3
        Field::new("c_birth_year", DataType::Int32, false),         // 4
        Field::new("c_birth_country", DataType::Utf8, false),       // 5
        Field::new("c_email_address", DataType::Utf8, false),       // 6
        Field::new("c_preferred_cust_flag", DataType::Utf8, false), // 7
        Field::new("c_current_addr_state", DataType::Utf8, false),  // 8
    ]));

    let countries = [
        "UNITED STATES", "CHINA", "INDIA", "JAPAN", "GERMANY",
        "BRAZIL", "CANADA", "MEXICO", "FRANCE", "UNITED KINGDOM",
    ];
    let first_names = [
        "James", "Mary", "Robert", "Patricia", "John",
        "Jennifer", "Michael", "Linda", "David", "Elizabeth",
    ];
    let last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones",
        "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
    ];

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                (0..n).map(|i| (i + 1) as i64).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n)
                    .map(|i| format!("AAAAAAAAB{}AAAA", i % 99999))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| first_names[i % first_names.len()]).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| last_names[i % last_names.len()]).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                (0..n).map(|i| 1940 + (i % 70) as i32).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| countries[i % countries.len()]).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n)
                    .map(|i| {
                        format!(
                            "{}.{}@example.com",
                            first_names[i % first_names.len()].to_lowercase(),
                            last_names[i % last_names.len()].to_lowercase()
                        )
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| if i % 3 == 0 { "Y" } else { "N" }).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| states[i % states.len()]).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// store dimension table (small)
fn make_store(n: usize) -> RecordBatch {
    let states = [
        "CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("s_store_sk", DataType::Int32, false),        // 0 — PK
        Field::new("s_store_name", DataType::Utf8, false),       // 1
        Field::new("s_state", DataType::Utf8, false),            // 2
        Field::new("s_market_id", DataType::Int32, false),       // 3
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                (0..n).map(|i| (i + 1) as i32).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| format!("Store #{:04}", i + 1)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| states[i % states.len()]).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                (0..n).map(|i| (i % 10 + 1) as i32).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

// ─── Benchmark runner ────────────────────────────────────────────────────────

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
            "  {:50} {:>10} rows  {:>8.2}ms  {:>8.2}M rows/s  → {} output",
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

// ─── TPC-DS Q3-like: Item-Brand Revenue Analysis ────────────────────────────
// SELECT i_brand, i_class, i_category,
//        SUM(ss_ext_sales_price) AS total_revenue,
//        AVG(ss_net_profit) AS avg_profit
// FROM store_sales
// JOIN date_dim ON ss_sold_date_sk = d_date_sk
// JOIN item ON ss_item_sk = i_item_sk
// WHERE d_year = 2000 AND i_category = 'Electronics'
// GROUP BY i_brand, i_class, i_category
// ORDER BY total_revenue DESC
// LIMIT 20

fn bench_ds_q3(
    store_sales: &RecordBatch,
    date_dim: &RecordBatch,
    item: &RecordBatch,
    label: &str,
) -> Vec<BenchResult> {
    let iters = if store_sales.num_rows() >= 1_000_000 { 3 } else { 10 };
    let mut results = Vec::new();

    // Step 1: Filter date_dim for year = 2000
    let date_pred = Predicate::Eq(1, ScalarValue::Int32(2000));
    let r = bench_op(
        &format!("DS-Q3 filter date_dim d_year=2000 [{}]", label),
        date_dim.num_rows(),
        iters,
        || {
            let mask = evaluate_predicate(date_dim, &date_pred).unwrap();
            filter_batch(date_dim, &mask).unwrap().num_rows()
        },
    );
    results.push(r);

    let date_mask = evaluate_predicate(date_dim, &date_pred).unwrap();
    let filtered_dates = filter_batch(date_dim, &date_mask).unwrap();

    // Step 2: Filter item for category = 'Electronics'
    let item_pred = Predicate::Eq(3, ScalarValue::Utf8("Electronics".into()));
    let r = bench_op(
        &format!("DS-Q3 filter item i_category=Elec [{}]", label),
        item.num_rows(),
        iters,
        || {
            let mask = evaluate_predicate(item, &item_pred).unwrap();
            filter_batch(item, &mask).unwrap().num_rows()
        },
    );
    results.push(r);

    let item_mask = evaluate_predicate(item, &item_pred).unwrap();
    let filtered_items = filter_batch(item, &item_mask).unwrap();

    // Step 3: Join store_sales → filtered_dates (star-schema fact→dim join)
    let join_date = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![0], // ss_sold_date_sk
        build_keys: vec![0], // d_date_sk
    };
    let r = bench_op(
        &format!("DS-Q3 join ss→date_dim [{}]", label),
        store_sales.num_rows(),
        iters,
        || hash_join(store_sales, &filtered_dates, &join_date).unwrap().num_rows(),
    );
    results.push(r);

    let ss_dates = hash_join(store_sales, &filtered_dates, &join_date).unwrap();

    // Step 4: Join (ss+date) → filtered_items (second star join)
    // After first join, ss_item_sk is still column 1 in the probe side
    let join_item = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![1], // ss_item_sk (from store_sales columns in joined result)
        build_keys: vec![0], // i_item_sk
    };
    let r = bench_op(
        &format!("DS-Q3 join (ss+date)→item [{}]", label),
        ss_dates.num_rows(),
        iters,
        || hash_join(&ss_dates, &filtered_items, &join_item).unwrap().num_rows(),
    );
    results.push(r);

    let ss_date_item = hash_join(&ss_dates, &filtered_items, &join_item).unwrap();

    // Step 5: Aggregate by (brand, class, category)
    // In joined result: store_sales(11 cols) + date_dim(6 cols) + item(8 cols)
    // item columns start at: 11 + 6 = 17
    // i_brand=17+5=22, i_class=17+4=21, i_category=17+3=20
    // ss_ext_sales_price=9, ss_net_profit=10
    let ncols = ss_date_item.num_columns();
    if ncols > 22 {
        let agg_config = HashAggConfig {
            group_by: vec![22, 21, 20], // i_brand, i_class, i_category
            aggregates: vec![
                AggExpr { column: 9, func: AggFunc::Sum, output_name: "total_revenue".into() },
                AggExpr { column: 10, func: AggFunc::Avg, output_name: "avg_profit".into() },
            ],
        };
        let r = bench_op(
            &format!("DS-Q3 agg by brand/class/cat [{}]", label),
            ss_date_item.num_rows(),
            iters,
            || hash_aggregate(&ss_date_item, &agg_config).unwrap().num_rows(),
        );
        results.push(r);

        // Step 6: Sort by revenue DESC, LIMIT 20
        let agg_result = hash_aggregate(&ss_date_item, &agg_config).unwrap();
        let sort_keys = vec![SortKey {
            column: 3, // total_revenue (after 3 group-by cols)
            direction: SortDirection::Desc,
            nulls: NullOrdering::NullsLast,
        }];
        let r = bench_op(
            &format!("DS-Q3 sort+limit20 [{}]", label),
            agg_result.num_rows(),
            iters,
            || sort_batch_limit(&agg_result, &sort_keys, 20).unwrap().num_rows(),
        );
        results.push(r);
    }

    results
}

// ─── TPC-DS Q7-like: Promotion Effect Analysis ──────────────────────────────
// SELECT i_item_id,
//        AVG(ss_quantity) AS agg1,
//        AVG(ss_list_price) AS agg2,
//        AVG(ss_sales_price) AS agg3,
//        AVG(ss_ext_sales_price) AS agg4
// FROM store_sales
// JOIN customer ON ss_customer_sk = c_customer_sk
// JOIN item ON ss_item_sk = i_item_sk
// WHERE c_birth_year BETWEEN 1970 AND 1980
// GROUP BY i_item_id
// ORDER BY i_item_id
// LIMIT 100

fn bench_ds_q7(
    store_sales: &RecordBatch,
    customer: &RecordBatch,
    item: &RecordBatch,
    label: &str,
) -> Vec<BenchResult> {
    let iters = if store_sales.num_rows() >= 1_000_000 { 3 } else { 10 };
    let mut results = Vec::new();

    // Step 1: Filter customer — birth_year BETWEEN 1970 AND 1980
    let cust_pred = Predicate::And(
        Box::new(Predicate::Ge(4, ScalarValue::Int32(1970))),
        Box::new(Predicate::Le(4, ScalarValue::Int32(1980))),
    );
    let r = bench_op(
        &format!("DS-Q7 filter cust year 1970-1980 [{}]", label),
        customer.num_rows(),
        iters,
        || {
            let mask = evaluate_predicate(customer, &cust_pred).unwrap();
            filter_batch(customer, &mask).unwrap().num_rows()
        },
    );
    results.push(r);

    let cust_mask = evaluate_predicate(customer, &cust_pred).unwrap();
    let filtered_cust = filter_batch(customer, &cust_mask).unwrap();

    // Step 2: Join store_sales → filtered_customer
    let join_cust = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![2], // ss_customer_sk
        build_keys: vec![0], // c_customer_sk
    };
    let r = bench_op(
        &format!("DS-Q7 join ss→customer [{}]", label),
        store_sales.num_rows(),
        iters,
        || hash_join(store_sales, &filtered_cust, &join_cust).unwrap().num_rows(),
    );
    results.push(r);

    let ss_cust = hash_join(store_sales, &filtered_cust, &join_cust).unwrap();

    // Step 3: Join (ss+cust) → item
    // ss_item_sk is column 1 in store_sales portion of joined result
    let join_item = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![1], // ss_item_sk
        build_keys: vec![0], // i_item_sk
    };
    let r = bench_op(
        &format!("DS-Q7 join (ss+cust)→item [{}]", label),
        ss_cust.num_rows(),
        iters,
        || hash_join(&ss_cust, &item, &join_item).unwrap().num_rows(),
    );
    results.push(r);

    let ss_cust_item = hash_join(&ss_cust, &item, &join_item).unwrap();

    // Step 4: Aggregate by i_item_id (high cardinality)
    // item columns start at: 11 (ss cols) + 9 (customer cols) = 20
    // i_item_id = 20 + 1 = 21
    let ncols = ss_cust_item.num_columns();
    if ncols > 21 {
        let agg_config = HashAggConfig {
            group_by: vec![21], // i_item_id — up to 18K distinct groups
            aggregates: vec![
                AggExpr { column: 5, func: AggFunc::Avg, output_name: "avg_quantity".into() },
                AggExpr { column: 7, func: AggFunc::Avg, output_name: "avg_list_price".into() },
                AggExpr { column: 8, func: AggFunc::Avg, output_name: "avg_sales_price".into() },
                AggExpr { column: 9, func: AggFunc::Avg, output_name: "avg_ext_sales".into() },
            ],
        };
        let r = bench_op(
            &format!("DS-Q7 agg by item_id (high card) [{}]", label),
            ss_cust_item.num_rows(),
            iters,
            || hash_aggregate(&ss_cust_item, &agg_config).unwrap().num_rows(),
        );
        results.push(r);

        // Step 5: Sort by item_id, limit 100
        let agg_result = hash_aggregate(&ss_cust_item, &agg_config).unwrap();
        let sort_keys = vec![SortKey {
            column: 0, // i_item_id (first col after group-by)
            direction: SortDirection::Asc,
            nulls: NullOrdering::NullsLast,
        }];
        let r = bench_op(
            &format!("DS-Q7 sort+limit100 [{}]", label),
            agg_result.num_rows(),
            iters,
            || sort_batch_limit(&agg_result, &sort_keys, 100).unwrap().num_rows(),
        );
        results.push(r);
    }

    results
}

// ─── TPC-DS Q19-like: Store Revenue by Customer State ────────────────────────
// SELECT i_brand, i_category, c_current_addr_state,
//        SUM(ss_ext_sales_price) AS revenue,
//        COUNT(*) AS cnt
// FROM store_sales
// JOIN item ON ss_item_sk = i_item_sk
// JOIN customer ON ss_customer_sk = c_customer_sk
// WHERE i_category IN ('Electronics', 'Books', 'Music')
//   AND c_current_addr_state IN ('CA', 'TX', 'NY', 'FL')
// GROUP BY i_brand, i_category, c_current_addr_state
// ORDER BY revenue DESC
// LIMIT 100
//
// Pattern: multi-way star join with filter on TWO dimension tables

fn bench_ds_q19(
    store_sales: &RecordBatch,
    item: &RecordBatch,
    customer: &RecordBatch,
    label: &str,
) -> Vec<BenchResult> {
    let iters = if store_sales.num_rows() >= 1_000_000 { 3 } else { 10 };
    let mut results = Vec::new();

    // Step 1: Filter item for category IN ('Electronics', 'Books', 'Music')
    let item_pred = Predicate::Or(
        Box::new(Predicate::Eq(3, ScalarValue::Utf8("Electronics".into()))),
        Box::new(Predicate::Or(
            Box::new(Predicate::Eq(3, ScalarValue::Utf8("Books".into()))),
            Box::new(Predicate::Eq(3, ScalarValue::Utf8("Music".into()))),
        )),
    );
    let item_mask = evaluate_predicate(item, &item_pred).unwrap();
    let filtered_item = filter_batch(item, &item_mask).unwrap();

    let r = bench_op(
        &format!("DS-Q19 filter item (3 categories) [{}]", label),
        item.num_rows(),
        iters,
        || {
            let mask = evaluate_predicate(item, &item_pred).unwrap();
            filter_batch(item, &mask).unwrap().num_rows()
        },
    );
    results.push(r);

    // Step 2: Filter customer for state IN ('CA', 'TX', 'NY', 'FL')
    let cust_pred = Predicate::Or(
        Box::new(Predicate::Eq(8, ScalarValue::Utf8("CA".into()))),
        Box::new(Predicate::Or(
            Box::new(Predicate::Eq(8, ScalarValue::Utf8("TX".into()))),
            Box::new(Predicate::Or(
                Box::new(Predicate::Eq(8, ScalarValue::Utf8("NY".into()))),
                Box::new(Predicate::Eq(8, ScalarValue::Utf8("FL".into()))),
            )),
        )),
    );
    let cust_mask = evaluate_predicate(customer, &cust_pred).unwrap();
    let filtered_cust = filter_batch(customer, &cust_mask).unwrap();

    let r = bench_op(
        &format!("DS-Q19 filter cust (4 states) [{}]", label),
        customer.num_rows(),
        iters,
        || {
            let mask = evaluate_predicate(customer, &cust_pred).unwrap();
            filter_batch(customer, &mask).unwrap().num_rows()
        },
    );
    results.push(r);

    // Step 3: Join ss → filtered_item
    let join_item = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![1], // ss_item_sk
        build_keys: vec![0], // i_item_sk
    };
    let ss_item = hash_join(store_sales, &filtered_item, &join_item).unwrap();
    let r = bench_op(
        &format!("DS-Q19 join ss→item [{}]", label),
        store_sales.num_rows(),
        iters,
        || hash_join(store_sales, &filtered_item, &join_item).unwrap().num_rows(),
    );
    results.push(r);

    // Step 4: Join (ss+item) → filtered_cust
    // ss_customer_sk is column 2 in store_sales portion
    let join_cust = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![2], // ss_customer_sk
        build_keys: vec![0], // c_customer_sk
    };
    let ss_item_cust = hash_join(&ss_item, &filtered_cust, &join_cust).unwrap();
    let r = bench_op(
        &format!("DS-Q19 join (ss+item)→cust [{}]", label),
        ss_item.num_rows(),
        iters,
        || hash_join(&ss_item, &filtered_cust, &join_cust).unwrap().num_rows(),
    );
    results.push(r);

    // Step 5: Aggregate by (i_brand, i_category, c_current_addr_state)
    // item cols start at 11 (ss cols): i_brand=11+5=16, i_category=11+3=14
    // customer cols start at 11+8=19: c_current_addr_state=19+8=27
    let ncols = ss_item_cust.num_columns();
    if ncols > 27 {
        let agg_config = HashAggConfig {
            group_by: vec![16, 14, 27], // i_brand, i_category, c_current_addr_state
            aggregates: vec![
                AggExpr { column: 9, func: AggFunc::Sum, output_name: "revenue".into() },
                AggExpr { column: 0, func: AggFunc::Count, output_name: "cnt".into() },
            ],
        };
        let r = bench_op(
            &format!("DS-Q19 agg by brand/cat/state [{}]", label),
            ss_item_cust.num_rows(),
            iters,
            || hash_aggregate(&ss_item_cust, &agg_config).unwrap().num_rows(),
        );
        results.push(r);

        let agg_result = hash_aggregate(&ss_item_cust, &agg_config).unwrap();
        let sort_keys = vec![SortKey {
            column: 3, // revenue
            direction: SortDirection::Desc,
            nulls: NullOrdering::NullsLast,
        }];
        let r = bench_op(
            &format!("DS-Q19 sort+limit100 [{}]", label),
            agg_result.num_rows(),
            iters,
            || sort_batch_limit(&agg_result, &sort_keys, 100).unwrap().num_rows(),
        );
        results.push(r);
    }

    results
}

// ─── TPC-DS Q27-like: Nested Aggregation (Avg of Avg) ───────────────────────
// WITH store_avg AS (
//   SELECT ss_store_sk, AVG(ss_ext_sales_price) AS avg_sales
//   FROM store_sales GROUP BY ss_store_sk
// )
// SELECT COUNT(*), AVG(avg_sales), SUM(avg_sales)
// FROM store_avg
// WHERE avg_sales > 200
//
// Pattern: aggregate, filter result, re-aggregate (nested/layered aggregation)

fn bench_ds_q27(store_sales: &RecordBatch, label: &str) -> Vec<BenchResult> {
    let iters = if store_sales.num_rows() >= 1_000_000 { 5 } else { 15 };
    let mut results = Vec::new();

    // Step 1: Group by ss_store_sk, compute AVG(ss_ext_sales_price)
    let agg1_config = HashAggConfig {
        group_by: vec![3], // ss_store_sk
        aggregates: vec![
            AggExpr { column: 9, func: AggFunc::Avg, output_name: "avg_sales".into() },
            AggExpr { column: 9, func: AggFunc::Sum, output_name: "total_sales".into() },
            AggExpr { column: 0, func: AggFunc::Count, output_name: "cnt".into() },
        ],
    };
    let r = bench_op(
        &format!("DS-Q27 agg by store_sk [{}]", label),
        store_sales.num_rows(),
        iters,
        || hash_aggregate(store_sales, &agg1_config).unwrap().num_rows(),
    );
    results.push(r);

    let store_avg = hash_aggregate(store_sales, &agg1_config).unwrap();

    // Step 2: Filter avg_sales > 200
    let pred = Predicate::Gt(1, ScalarValue::Float64(200.0));
    let r = bench_op(
        &format!("DS-Q27 filter avg_sales>200 [{}]", label),
        store_avg.num_rows(),
        iters,
        || {
            let mask = evaluate_predicate(&store_avg, &pred).unwrap();
            filter_batch(&store_avg, &mask).unwrap().num_rows()
        },
    );
    results.push(r);

    let filtered_mask = evaluate_predicate(&store_avg, &pred).unwrap();
    let filtered = filter_batch(&store_avg, &filtered_mask).unwrap();

    // Step 3: Global re-aggregate
    let agg2_config = HashAggConfig {
        group_by: vec![],
        aggregates: vec![
            AggExpr { column: 0, func: AggFunc::Count, output_name: "store_count".into() },
            AggExpr { column: 1, func: AggFunc::Avg, output_name: "avg_of_avg".into() },
            AggExpr { column: 2, func: AggFunc::Sum, output_name: "sum_total".into() },
        ],
    };
    let r = bench_op(
        &format!("DS-Q27 nested global agg [{}]", label),
        filtered.num_rows(),
        iters,
        || hash_aggregate(&filtered, &agg2_config).unwrap().num_rows(),
    );
    results.push(r);

    results
}

// ─── TPC-DS Q43-like: Store Revenue by Day of Week ──────────────────────────
// SELECT s_store_name, d_day_name,
//        SUM(ss_sales_price) AS total
// FROM store_sales
// JOIN date_dim ON ss_sold_date_sk = d_date_sk
// JOIN store ON ss_store_sk = s_store_sk
// WHERE d_year = 2000
// GROUP BY s_store_name, d_day_name
// ORDER BY total DESC
// LIMIT 100
//
// Pattern: 3-way join with GROUP BY on text columns (string-heavy grouping)

fn bench_ds_q43(
    store_sales: &RecordBatch,
    date_dim: &RecordBatch,
    store: &RecordBatch,
    label: &str,
) -> Vec<BenchResult> {
    let iters = if store_sales.num_rows() >= 1_000_000 { 3 } else { 10 };
    let mut results = Vec::new();

    // Step 1: Filter date_dim for d_year = 2000
    let date_pred = Predicate::Eq(1, ScalarValue::Int32(2000));
    let date_mask = evaluate_predicate(date_dim, &date_pred).unwrap();
    let filtered_dates = filter_batch(date_dim, &date_mask).unwrap();

    // Step 2: Join ss → date_dim
    let join_date = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![0], // ss_sold_date_sk
        build_keys: vec![0], // d_date_sk
    };
    let ss_date = hash_join(store_sales, &filtered_dates, &join_date).unwrap();
    let r = bench_op(
        &format!("DS-Q43 join ss→date_dim [{}]", label),
        store_sales.num_rows(),
        iters,
        || hash_join(store_sales, &filtered_dates, &join_date).unwrap().num_rows(),
    );
    results.push(r);

    // Step 3: Join (ss+date) → store (Int32 key join)
    // ss_store_sk is column 3 in store_sales portion of join result
    // store.s_store_sk is column 0 (Int32)
    let join_store = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![3], // ss_store_sk
        build_keys: vec![0], // s_store_sk
    };
    let ss_date_store = hash_join(&ss_date, store, &join_store).unwrap();
    let r = bench_op(
        &format!("DS-Q43 join (ss+date)→store [{}]", label),
        ss_date.num_rows(),
        iters,
        || hash_join(&ss_date, store, &join_store).unwrap().num_rows(),
    );
    results.push(r);

    // Step 4: Aggregate by (s_store_name, d_day_name) — text grouping
    // ss cols: 11, date_dim cols: 6 → store cols start at 17
    // s_store_name = 17 + 1 = 18
    // d_day_name: date_dim starts at col 11, d_day_name = 11 + 5 = 16
    // ss_sales_price = 8
    let ncols = ss_date_store.num_columns();
    if ncols > 18 {
        let agg_config = HashAggConfig {
            group_by: vec![18, 16], // s_store_name, d_day_name
            aggregates: vec![
                AggExpr { column: 8, func: AggFunc::Sum, output_name: "total_sales".into() },
            ],
        };
        let r = bench_op(
            &format!("DS-Q43 agg by store_name/day [{}]", label),
            ss_date_store.num_rows(),
            iters,
            || hash_aggregate(&ss_date_store, &agg_config).unwrap().num_rows(),
        );
        results.push(r);

        let agg_result = hash_aggregate(&ss_date_store, &agg_config).unwrap();
        let sort_keys = vec![SortKey {
            column: 2, // total_sales
            direction: SortDirection::Desc,
            nulls: NullOrdering::NullsLast,
        }];
        let r = bench_op(
            &format!("DS-Q43 sort+limit100 [{}]", label),
            agg_result.num_rows(),
            iters,
            || sort_batch_limit(&agg_result, &sort_keys, 100).unwrap().num_rows(),
        );
        results.push(r);
    }

    results
}

// ─── TPC-DS Q55-like: Brand/Month Revenue with String Ops ───────────────────
// SELECT i_brand, d_moy,
//        SUM(ss_ext_sales_price) AS revenue
// FROM store_sales
// JOIN date_dim ON ss_sold_date_sk = d_date_sk
// JOIN item ON ss_item_sk = i_item_sk
// WHERE d_year = 2001 AND d_moy = 11
//   AND i_manager_id = 28
//   AND i_brand LIKE '%amalg%'
// GROUP BY i_brand, d_moy
// ORDER BY revenue DESC
// LIMIT 100
//
// Pattern: LIKE filter + multi-join + aggregate — stresses string operations

fn bench_ds_q55(
    store_sales: &RecordBatch,
    date_dim: &RecordBatch,
    item: &RecordBatch,
    label: &str,
) -> Vec<BenchResult> {
    let iters = if store_sales.num_rows() >= 1_000_000 { 3 } else { 10 };
    let mut results = Vec::new();

    // Step 1: Filter date_dim: year=2001, month=11
    let date_pred = Predicate::And(
        Box::new(Predicate::Eq(1, ScalarValue::Int32(2001))),
        Box::new(Predicate::Eq(2, ScalarValue::Int32(11))),
    );
    let date_mask = evaluate_predicate(date_dim, &date_pred).unwrap();
    let filtered_dates = filter_batch(date_dim, &date_mask).unwrap();

    // Step 2: Filter item: i_manager_id = 28 AND i_brand LIKE '%amalg%'
    // First the Int32 predicate
    let mgr_pred = Predicate::Eq(7, ScalarValue::Int32(28));
    let mgr_mask = evaluate_predicate(item, &mgr_pred).unwrap();
    let item_mgr = filter_batch(item, &mgr_mask).unwrap();

    // Then LIKE on the brand column
    let brand_col = item_mgr
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let r = bench_op(
        &format!("DS-Q55 LIKE '%amalg%' on brand [{}]", label),
        item_mgr.num_rows(),
        iters,
        || {
            let like_mask = string_like(brand_col, "%amalg%").unwrap();
            like_mask.iter().filter(|v| v == &Some(true)).count()
        },
    );
    results.push(r);

    let like_mask = string_like(brand_col, "%amalg%").unwrap();
    let filtered_item = filter_batch(&item_mgr, &like_mask).unwrap();

    // Step 3: Join ss → filtered_dates
    let join_date = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![0],
        build_keys: vec![0],
    };
    let ss_date = hash_join(store_sales, &filtered_dates, &join_date).unwrap();
    let r = bench_op(
        &format!("DS-Q55 join ss→date_dim [{}]", label),
        store_sales.num_rows(),
        iters,
        || hash_join(store_sales, &filtered_dates, &join_date).unwrap().num_rows(),
    );
    results.push(r);

    // Step 4: Join (ss+date) → filtered_item
    let join_item = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![1],
        build_keys: vec![0],
    };
    let ss_date_item = hash_join(&ss_date, &filtered_item, &join_item).unwrap();
    let r = bench_op(
        &format!("DS-Q55 join (ss+date)→item [{}]", label),
        ss_date.num_rows(),
        iters,
        || hash_join(&ss_date, &filtered_item, &join_item).unwrap().num_rows(),
    );
    results.push(r);

    // Step 5: Aggregate by (i_brand, d_moy)
    // item starts at 11+6=17, i_brand=17+5=22
    // d_moy: date cols start at 11, d_moy=11+2=13
    let ncols = ss_date_item.num_columns();
    if ncols > 22 {
        let agg_config = HashAggConfig {
            group_by: vec![22, 13], // i_brand, d_moy
            aggregates: vec![
                AggExpr { column: 9, func: AggFunc::Sum, output_name: "revenue".into() },
            ],
        };
        let r = bench_op(
            &format!("DS-Q55 agg by brand/month [{}]", label),
            ss_date_item.num_rows(),
            iters,
            || hash_aggregate(&ss_date_item, &agg_config).unwrap().num_rows(),
        );
        results.push(r);
    }

    results
}

// ─── TPC-DS Q96-like: String-Heavy Customer Analysis ─────────────────────────
// Exercises string operations: LIKE, CONTAINS, UPPER, SUBSTRING
// on customer dimension before joining to fact table.
//
// Pattern: heavy string processing on dimension → join → aggregate

fn bench_ds_q96(
    store_sales: &RecordBatch,
    customer: &RecordBatch,
    label: &str,
) -> Vec<BenchResult> {
    let iters = if store_sales.num_rows() >= 1_000_000 { 3 } else { 10 };
    let mut results = Vec::new();

    // Step 1: String operations on customer email
    let email_col = customer
        .column(6)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let r = bench_op(
        &format!("DS-Q96 LIKE email '%example%' [{}]", label),
        customer.num_rows(),
        iters,
        || {
            let mask = string_like(email_col, "%example%").unwrap();
            mask.iter().filter(|v| v == &Some(true)).count()
        },
    );
    results.push(r);

    let r = bench_op(
        &format!("DS-Q96 UPPER(first_name) [{}]", label),
        customer.num_rows(),
        iters,
        || {
            let name_col = customer.column(2).as_any().downcast_ref::<StringArray>().unwrap();
            string_upper(name_col).unwrap().len()
        },
    );
    results.push(r);

    let r = bench_op(
        &format!("DS-Q96 SUBSTRING(email,1,5) [{}]", label),
        customer.num_rows(),
        iters,
        || {
            string_substring(email_col, 1, Some(5)).unwrap().len()
        },
    );
    results.push(r);

    let r = bench_op(
        &format!("DS-Q96 CONTAINS(email,'smith') [{}]", label),
        customer.num_rows(),
        iters,
        || {
            let mask = string_contains(email_col, "smith").unwrap();
            mask.iter().filter(|v| v == &Some(true)).count()
        },
    );
    results.push(r);

    // Step 2: Filter customers by preferred flag + join to fact
    let cust_pred = Predicate::Eq(7, ScalarValue::Utf8("Y".into()));
    let cust_mask = evaluate_predicate(customer, &cust_pred).unwrap();
    let filtered_cust = filter_batch(customer, &cust_mask).unwrap();

    let join_cust = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![2], // ss_customer_sk
        build_keys: vec![0], // c_customer_sk
    };
    let r = bench_op(
        &format!("DS-Q96 join ss→preferred_cust [{}]", label),
        store_sales.num_rows(),
        iters,
        || hash_join(store_sales, &filtered_cust, &join_cust).unwrap().num_rows(),
    );
    results.push(r);

    // Step 3: Aggregate by state
    let ss_cust = hash_join(store_sales, &filtered_cust, &join_cust).unwrap();
    // c_current_addr_state: customer col 8 → joined at 11 + 8 = 19
    let ncols = ss_cust.num_columns();
    if ncols > 19 {
        let agg_config = HashAggConfig {
            group_by: vec![19], // c_current_addr_state
            aggregates: vec![
                AggExpr { column: 9, func: AggFunc::Sum, output_name: "revenue".into() },
                AggExpr { column: 0, func: AggFunc::Count, output_name: "cnt".into() },
                AggExpr { column: 10, func: AggFunc::Avg, output_name: "avg_profit".into() },
            ],
        };
        let r = bench_op(
            &format!("DS-Q96 agg by state [{}]", label),
            ss_cust.num_rows(),
            iters,
            || hash_aggregate(&ss_cust, &agg_config).unwrap().num_rows(),
        );
        results.push(r);
    }

    results
}

// ─── Main benchmark entry ────────────────────────────────────────────────────

fn main() {
    eprintln!("╔══════════════════════════════════════════════════════════════════════════════╗");
    eprintln!("║           Prism TPC-DS Operator Benchmark (release build)                    ║");
    eprintln!("╚══════════════════════════════════════════════════════════════════════════════╝");
    eprintln!();
    eprintln!("Patterns tested: star-schema joins, multi-way joins, nested aggregation,");
    eprintln!("high-cardinality GROUP BY, string LIKE/UPPER/SUBSTRING, text grouping");
    eprintln!();

    // TPC-DS scale factors:
    //   SF1  ≈ 2.8M store_sales, 18K items, 100K customers, 73K dates, 402 stores
    //   SF10 ≈ 28M store_sales
    for &(ss_size, item_size, cust_size, date_size, store_size, label) in &[
        (500_000, 4_500, 25_000, 730, 100, "SF~0.2"),
        (2_880_000, 18_000, 100_000, 2190, 400, "SF~1"),
    ] {
        eprintln!("━━━ {} (ss={}, item={}, cust={}, date={}, store={}) ━━━",
            label, ss_size, item_size, cust_size, date_size, store_size);

        let t0 = Instant::now();
        let store_sales = make_store_sales(ss_size);
        let date_dim = make_date_dim(date_size);
        let item = make_item(item_size);
        let customer = make_customer(cust_size);
        let store = make_store(store_size);
        eprintln!("  Data generation: {:.0}ms", t0.elapsed().as_millis());
        eprintln!();

        // Q3-like: Brand Revenue (star-schema + filter + agg + sort)
        eprintln!("  DS-Q3 (Brand Revenue — 2-dim star join + agg):");
        for r in bench_ds_q3(&store_sales, &date_dim, &item, label) {
            r.print();
        }
        eprintln!();

        // Q7-like: Promotion Effect (high-cardinality grouping)
        eprintln!("  DS-Q7 (Promotion Effect — high-card group-by):");
        for r in bench_ds_q7(&store_sales, &customer, &item, label) {
            r.print();
        }
        eprintln!();

        // Q19-like: Store Revenue by State (multi-way with filters on 2 dims)
        eprintln!("  DS-Q19 (Store Revenue by State — multi-dim filter + join):");
        for r in bench_ds_q19(&store_sales, &item, &customer, label) {
            r.print();
        }
        eprintln!();

        // Q27-like: Nested Aggregation
        eprintln!("  DS-Q27 (Nested Aggregation — agg → filter → re-agg):");
        for r in bench_ds_q27(&store_sales, label) {
            r.print();
        }
        eprintln!();

        // Q43-like: Store by Day of Week (string-heavy grouping)
        eprintln!("  DS-Q43 (Store by Day — 3-way join + text GROUP BY):");
        for r in bench_ds_q43(&store_sales, &date_dim, &store, label) {
            r.print();
        }
        eprintln!();

        // Q55-like: Brand/Month with LIKE (string ops)
        eprintln!("  DS-Q55 (Brand/Month — LIKE filter + multi-join):");
        for r in bench_ds_q55(&store_sales, &date_dim, &item, label) {
            r.print();
        }
        eprintln!();

        // Q96-like: Customer String Analysis
        eprintln!("  DS-Q96 (Customer String Ops — LIKE/UPPER/SUBSTR/CONTAINS):");
        for r in bench_ds_q96(&store_sales, &customer, label) {
            r.print();
        }
        eprintln!();
    }

    eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    eprintln!("TPC-DS Key Patterns vs Trino JVM Baseline (SF1, single-thread estimates):");
    eprintln!("  Star-schema 2-dim join + agg:   Trino ~3-6s    (serialization + DefaultPagesHash)");
    eprintln!("  High-cardinality GROUP BY:       Trino ~4-8s    (BigintGroupByHash + rehashing)");
    eprintln!("  Multi-dim filter + join:          Trino ~5-10s   (per-row predicate + join)");
    eprintln!("  Nested aggregation:               Trino ~1-3s    (intermediate materialization)");
    eprintln!("  3-way join + text GROUP BY:       Trino ~4-8s    (string hashing + comparison)");
    eprintln!("  LIKE + multi-join:                Trino ~3-7s    (VariableWidthBlock scan + join)");
    eprintln!("  String ops (LIKE/UPPER/SUBSTR):   Trino ~2-5s    (per-character Java processing)");
    eprintln!();
    eprintln!("Expected Prism speedup: 5-20x on these patterns due to:");
    eprintln!("  • Arrow columnar layout → better cache locality");
    eprintln!("  • SIMD vectorized string/filter kernels");
    eprintln!("  • Zero-copy hash tables on Arrow arrays");
    eprintln!("  • No per-row object boxing/unboxing");
}
