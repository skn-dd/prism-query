//! Execution plan types — the intermediate representation between Substrait
//! deserialization and actual Arrow execution.

use arrow_schema::SchemaRef;
use prism_executor::filter_project::{Predicate, ScalarExpr};
use prism_executor::hash_aggregate::AggExpr;
use prism_executor::hash_join::JoinType;
use prism_executor::sort::SortKey;

/// A node in the execution plan tree.
#[derive(Debug, Clone)]
pub enum PlanNode {
    /// Table scan — leaf node that reads from a data source.
    Scan {
        table_name: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    },

    /// Filter operator — applies a predicate.
    Filter {
        input: Box<PlanNode>,
        predicate: Predicate,
    },

    /// Projection — selects existing columns and computes new ones.
    Project {
        input: Box<PlanNode>,
        columns: Vec<usize>,
        expressions: Vec<ScalarExpr>,
    },

    /// Hash aggregation.
    Aggregate {
        input: Box<PlanNode>,
        group_by: Vec<usize>,
        aggregates: Vec<AggExpr>,
    },

    /// Hash join.
    Join {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinType,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
    },

    /// Sort / Order By.
    Sort {
        input: Box<PlanNode>,
        sort_keys: Vec<SortKey>,
        limit: Option<usize>,
    },

    /// Arrow Flight exchange — shuffle data between workers.
    Exchange {
        input: Box<PlanNode>,
        partition_keys: Vec<usize>,
        target_endpoints: Vec<String>,
    },
}

impl PlanNode {
    /// For aggregate nodes, return the output name of the first aggregate expression.
    pub fn aggregate_output_name(&self) -> Option<String> {
        if let PlanNode::Aggregate { aggregates, .. } = self {
            aggregates.first().map(|a| a.output_name.clone())
        } else {
            None
        }
    }

    /// Remap column indices throughout the plan tree using a mapping from
    /// original column indices to new (projected) indices.
    /// Used when loading projected Parquet data to avoid the expensive
    /// expand_projected_batches step.
    pub fn remap_columns(&mut self, col_map: &std::collections::HashMap<usize, usize>) {
        match self {
            PlanNode::Scan { projection, .. } => {
                if let Some(proj) = projection {
                    for idx in proj.iter_mut() {
                        if let Some(&new_idx) = col_map.get(idx) {
                            *idx = new_idx;
                        }
                    }
                }
            }
            PlanNode::Filter { input, predicate } => {
                remap_predicate(predicate, col_map);
                input.remap_columns(col_map);
            }
            PlanNode::Project { input, columns, expressions } => {
                for idx in columns.iter_mut() {
                    if let Some(&new_idx) = col_map.get(idx) {
                        *idx = new_idx;
                    }
                }
                for expr in expressions.iter_mut() {
                    remap_expr(expr, col_map);
                }
                input.remap_columns(col_map);
            }
            PlanNode::Aggregate { input, group_by, aggregates } => {
                for idx in group_by.iter_mut() {
                    if let Some(&new_idx) = col_map.get(idx) {
                        *idx = new_idx;
                    }
                }
                for agg in aggregates.iter_mut() {
                    if let Some(&new_idx) = col_map.get(&agg.column) {
                        agg.column = new_idx;
                    }
                }
                input.remap_columns(col_map);
            }
            PlanNode::Join { left, right, left_keys, right_keys, .. } => {
                for idx in left_keys.iter_mut() {
                    if let Some(&new_idx) = col_map.get(idx) {
                        *idx = new_idx;
                    }
                }
                for idx in right_keys.iter_mut() {
                    if let Some(&new_idx) = col_map.get(idx) {
                        *idx = new_idx;
                    }
                }
                left.remap_columns(col_map);
                right.remap_columns(col_map);
            }
            PlanNode::Sort { input, sort_keys, .. } => {
                for key in sort_keys.iter_mut() {
                    if let Some(&new_idx) = col_map.get(&key.column) {
                        key.column = new_idx;
                    }
                }
                input.remap_columns(col_map);
            }
            PlanNode::Exchange { input, partition_keys, .. } => {
                for idx in partition_keys.iter_mut() {
                    if let Some(&new_idx) = col_map.get(idx) {
                        *idx = new_idx;
                    }
                }
                input.remap_columns(col_map);
            }
        }
    }
}

fn remap_predicate(pred: &mut Predicate, col_map: &std::collections::HashMap<usize, usize>) {
    use Predicate::*;
    match pred {
        Eq(c, _) | Ne(c, _) | Lt(c, _) | Le(c, _) | Gt(c, _) | Ge(c, _) => {
            if let Some(&new_idx) = col_map.get(c) { *c = new_idx; }
        }
        IsNull(c) | IsNotNull(c) => {
            if let Some(&new_idx) = col_map.get(c) { *c = new_idx; }
        }
        Like(c, _) | ILike(c, _) => {
            if let Some(&new_idx) = col_map.get(c) { *c = new_idx; }
        }
        And(l, r) | Or(l, r) => {
            remap_predicate(l, col_map);
            remap_predicate(r, col_map);
        }
        Not(inner) => {
            remap_predicate(inner, col_map);
        }
    }
}

fn remap_expr(expr: &mut ScalarExpr, col_map: &std::collections::HashMap<usize, usize>) {
    use ScalarExpr::*;
    match expr {
        ColumnRef(c) => {
            if let Some(&new_idx) = col_map.get(c) { *c = new_idx; }
        }
        Literal(_) => {}
        BinaryOp { left, right, .. } => {
            remap_expr(left, col_map);
            remap_expr(right, col_map);
        }
        Negate(inner) => {
            remap_expr(inner, col_map);
        }
    }
}

/// A complete execution plan with metadata.
#[derive(Debug)]
pub struct ExecutionPlan {
    /// Root of the plan tree.
    pub root: PlanNode,
    /// Output schema.
    pub output_schema: SchemaRef,
}
