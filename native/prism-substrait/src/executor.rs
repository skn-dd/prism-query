//! Plan executor — walks a PlanNode tree and executes it against a table registry.
//!
//! Extracted from prism-jni to be reusable by both the JNI bridge and
//! the Arrow Flight worker path.

use std::collections::HashMap;

use arrow::compute::concat_batches;
use arrow_array::RecordBatch;

use prism_executor::filter_project;
use prism_executor::hash_aggregate;
use prism_executor::hash_join;
use prism_executor::sort;

use crate::plan::PlanNode;
use crate::{Result, SubstraitError};

/// Execute a PlanNode tree against a table registry.
///
/// The `tables` map provides the data for Scan nodes: table_name → RecordBatch.
/// This is the main entry point for both JNI and Flight-based execution.
pub fn execute_plan(
    node: &PlanNode,
    tables: &HashMap<String, RecordBatch>,
) -> Result<RecordBatch> {
    let batches = execute_node(node, tables)?;
    if batches.is_empty() {
        return Err(SubstraitError::Internal("plan produced no output".into()));
    }
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }
    let schema = batches[0].schema();
    Ok(concat_batches(&schema, &batches)?)
}

fn execute_node(
    node: &PlanNode,
    tables: &HashMap<String, RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    match node {
        PlanNode::Scan { table_name, projection, .. } => {
            let batch = tables.get(table_name).ok_or_else(|| {
                SubstraitError::Internal(format!("table '{}' not found in registry", table_name))
            })?;
            // Apply projection if specified
            if let Some(proj_cols) = projection {
                let projected = filter_project::project_batch(batch, proj_cols)?;
                Ok(vec![projected])
            } else {
                Ok(vec![batch.clone()])
            }
        }

        PlanNode::Filter { input, predicate } => {
            let child_batches = execute_node(input, tables)?;
            let mut output = Vec::new();
            for batch in &child_batches {
                let mask = filter_project::evaluate_predicate(batch, predicate)?;
                output.push(filter_project::filter_batch(batch, &mask)?);
            }
            Ok(output)
        }

        PlanNode::Project { input, columns, expressions } => {
            let child_batches = execute_node(input, tables)?;
            let mut output = Vec::new();
            for batch in &child_batches {
                if expressions.is_empty() {
                    output.push(filter_project::project_batch(batch, columns)?);
                } else {
                    output.push(filter_project::project_batch_with_exprs(batch, columns, expressions)?);
                }
            }
            Ok(output)
        }

        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let child_batches = execute_node(input, tables)?;
            if child_batches.is_empty() {
                return Ok(vec![]);
            }
            let merged = concat_batches(&child_batches[0].schema(), &child_batches)?;

            let config = hash_aggregate::HashAggConfig {
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
            };
            let result = hash_aggregate::hash_aggregate(&merged, &config)?;
            Ok(vec![result])
        }

        PlanNode::Join {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
        } => {
            let left_batches = execute_node(left, tables)?;
            let right_batches = execute_node(right, tables)?;

            if left_batches.is_empty() || right_batches.is_empty() {
                return Ok(vec![]);
            }

            let left_merged =
                concat_batches(&left_batches[0].schema(), &left_batches)?;
            let right_merged =
                concat_batches(&right_batches[0].schema(), &right_batches)?;

            let config = hash_join::HashJoinConfig {
                join_type: *join_type,
                probe_keys: left_keys.clone(),
                build_keys: right_keys.clone(),
            };
            let result = hash_join::hash_join(&left_merged, &right_merged, &config)?;
            Ok(vec![result])
        }

        PlanNode::Sort {
            input,
            sort_keys,
            limit,
        } => {
            let child_batches = execute_node(input, tables)?;
            if child_batches.is_empty() {
                return Ok(vec![]);
            }
            let merged = concat_batches(&child_batches[0].schema(), &child_batches)?;

            let result = if let Some(lim) = limit {
                sort::sort_batch_limit(&merged, sort_keys, *lim)?
            } else {
                sort::sort_batch(&merged, sort_keys)?
            };
            Ok(vec![result])
        }

        PlanNode::Exchange { input, .. } => {
            // Exchange is handled at the Flight layer, not in-process
            execute_node(input, tables)
        }
    }
}
