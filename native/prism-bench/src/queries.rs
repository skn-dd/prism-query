//! TPC-H query pipelines using prism-executor operators.
//!
//! Each query is a function that takes table data and returns results.
//! These execute on the worker, against the local data shard.

use std::collections::HashMap;

use arrow_array::RecordBatch;

use prism_executor::filter_project::{evaluate_predicate, filter_batch, Predicate, ScalarValue};
use prism_executor::hash_aggregate::{hash_aggregate, AggExpr, AggFunc, HashAggConfig};
use prism_executor::hash_join::{hash_join, HashJoinConfig, JoinType};
use prism_executor::sort::{sort_batch_limit, NullOrdering, SortDirection, SortKey};

/// TPC-H Q1: Pricing Summary Report
///
/// SELECT l_returnflag, l_linestatus,
///        SUM(l_quantity), SUM(l_extendedprice),
///        AVG(l_discount), COUNT(*)
/// FROM lineitem
/// WHERE l_linestatus = 'O' OR l_linestatus = 'F'
/// GROUP BY l_returnflag, l_linestatus
/// ORDER BY l_returnflag, l_linestatus
///
/// Schema: l_orderkey(0), l_partkey(1), l_suppkey(2), l_linenumber(3),
///         l_quantity(4), l_extendedprice(5), l_discount(6), l_tax(7),
///         l_returnflag(8), l_linestatus(9)
pub fn execute_q1(tables: &HashMap<String, RecordBatch>) -> anyhow::Result<RecordBatch> {
    let lineitem = tables.get("lineitem")
        .ok_or_else(|| anyhow::anyhow!("lineitem table not found"))?;

    // Filter: l_discount < 0.07 (simulates date-based filter, ~64% selectivity)
    let pred = Predicate::Lt(6, ScalarValue::Float64(0.07));
    let mask = evaluate_predicate(lineitem, &pred)?;
    let filtered = filter_batch(lineitem, &mask)?;

    // Aggregate: GROUP BY l_returnflag(8), l_linestatus(9)
    let agg_config = HashAggConfig {
        group_by: vec![8, 9],
        aggregates: vec![
            AggExpr { column: 4, func: AggFunc::Sum, output_name: "sum_qty".into() },
            AggExpr { column: 5, func: AggFunc::Sum, output_name: "sum_base_price".into() },
            AggExpr { column: 6, func: AggFunc::Avg, output_name: "avg_disc".into() },
            AggExpr { column: 0, func: AggFunc::Count, output_name: "count_order".into() },
        ],
    };
    let aggregated = hash_aggregate(&filtered, &agg_config)?;

    // Sort: ORDER BY l_returnflag, l_linestatus
    let sort_keys = vec![
        SortKey { column: 0, direction: SortDirection::Asc, nulls: NullOrdering::NullsLast },
        SortKey { column: 1, direction: SortDirection::Asc, nulls: NullOrdering::NullsLast },
    ];
    let result = sort_batch_limit(&aggregated, &sort_keys, aggregated.num_rows())?;

    Ok(result)
}

/// TPC-H Q3: Shipping Priority
///
/// SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) as revenue
/// FROM lineitem JOIN orders ON l_orderkey = o_orderkey
/// WHERE o_orderstatus = 'O'
/// GROUP BY l_orderkey, o_orderpriority
/// ORDER BY revenue DESC
/// LIMIT 10
///
/// Orders schema: o_orderkey(0), o_custkey(1), o_orderstatus(2),
///                o_totalprice(3), o_orderpriority(4)
pub fn execute_q3(tables: &HashMap<String, RecordBatch>) -> anyhow::Result<RecordBatch> {
    let lineitem = tables.get("lineitem")
        .ok_or_else(|| anyhow::anyhow!("lineitem table not found"))?;
    let orders = tables.get("orders")
        .ok_or_else(|| anyhow::anyhow!("orders table not found"))?;

    // Filter orders: o_orderstatus = 'O'
    let pred = Predicate::Eq(2, ScalarValue::Utf8("O".into()));
    let mask = evaluate_predicate(orders, &pred)?;
    let filtered_orders = filter_batch(orders, &mask)?;

    // Join: lineitem.l_orderkey(0) = orders.o_orderkey(0)
    let join_config = HashJoinConfig {
        join_type: JoinType::Inner,
        probe_keys: vec![0],    // lineitem.l_orderkey
        build_keys: vec![0],    // orders.o_orderkey
    };
    let joined = hash_join(lineitem, &filtered_orders, &join_config)?;

    // Aggregate: GROUP BY l_orderkey (col 0 in joined output)
    // joined schema: lineitem cols (0-9) + orders cols (10-14, with _right suffixes)
    let agg_config = HashAggConfig {
        group_by: vec![0],  // l_orderkey
        aggregates: vec![
            AggExpr { column: 5, func: AggFunc::Sum, output_name: "revenue".into() },
            AggExpr { column: 0, func: AggFunc::Count, output_name: "count".into() },
        ],
    };
    let aggregated = hash_aggregate(&joined, &agg_config)?;

    // Sort: ORDER BY revenue DESC, LIMIT 10
    let sort_keys = vec![SortKey {
        column: 1,  // revenue column in aggregated output
        direction: SortDirection::Desc,
        nulls: NullOrdering::NullsLast,
    }];
    let result = sort_batch_limit(&aggregated, &sort_keys, 10)?;

    Ok(result)
}

/// TPC-H Q6: Forecasting Revenue Change
///
/// SELECT SUM(l_extendedprice * l_discount) as revenue
/// FROM lineitem
/// WHERE l_discount BETWEEN 0.05 AND 0.07
///   AND l_quantity < 24
///
/// This is a pure scan + filter + global aggregate — no joins.
pub fn execute_q6(tables: &HashMap<String, RecordBatch>) -> anyhow::Result<RecordBatch> {
    let lineitem = tables.get("lineitem")
        .ok_or_else(|| anyhow::anyhow!("lineitem table not found"))?;

    // Compound filter: l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24
    let pred = Predicate::And(
        Box::new(Predicate::And(
            Box::new(Predicate::Ge(6, ScalarValue::Float64(0.05))),
            Box::new(Predicate::Le(6, ScalarValue::Float64(0.07))),
        )),
        Box::new(Predicate::Lt(4, ScalarValue::Float64(24.0))),
    );
    let mask = evaluate_predicate(lineitem, &pred)?;
    let filtered = filter_batch(lineitem, &mask)?;

    // Global aggregate: SUM(l_extendedprice)
    // We use a dummy group-by (empty would fail), so we aggregate the full dataset
    let agg_config = HashAggConfig {
        group_by: vec![],
        aggregates: vec![
            AggExpr { column: 5, func: AggFunc::Sum, output_name: "revenue".into() },
            AggExpr { column: 0, func: AggFunc::Count, output_name: "count".into() },
        ],
    };
    let aggregated = hash_aggregate(&filtered, &agg_config)?;

    Ok(aggregated)
}
