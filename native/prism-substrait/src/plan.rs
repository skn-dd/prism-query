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

/// A complete execution plan with metadata.
#[derive(Debug)]
pub struct ExecutionPlan {
    /// Root of the plan tree.
    pub root: PlanNode,
    /// Output schema.
    pub output_schema: SchemaRef,
}
