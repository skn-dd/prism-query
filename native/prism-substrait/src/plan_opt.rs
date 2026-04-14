//! Plan optimization — extract scan hints for Parquet pushdown.
//!
//! Walks the plan tree to find Filter predicates adjacent to Scan nodes,
//! returning hints that the table loader can use for row group skipping.

use std::collections::HashMap;

use prism_executor::filter_project::Predicate;

use crate::plan::PlanNode;

/// Hints extracted from the plan tree for a specific table scan.
#[derive(Debug, Clone)]
pub struct ScanHint {
    /// Filter predicate to push down to Parquet row group skipping.
    pub predicate: Option<Predicate>,
}

/// Walk the plan tree and extract hints for each Scan node.
///
/// When a Filter sits directly above a Scan (or above a Project above a Scan),
/// the predicate is captured for use during Parquet loading.
pub fn extract_scan_hints(node: &PlanNode) -> HashMap<String, ScanHint> {
    let mut hints = HashMap::new();
    extract_hints_recursive(node, &mut hints);
    hints
}

fn extract_hints_recursive(node: &PlanNode, hints: &mut HashMap<String, ScanHint>) {
    match node {
        PlanNode::Filter { input, predicate } => {
            // Check if the Filter's child is a Scan (or Project -> Scan)
            match input.as_ref() {
                PlanNode::Scan { table_name, .. } => {
                    hints.insert(
                        table_name.clone(),
                        ScanHint {
                            predicate: Some(predicate.clone()),
                        },
                    );
                }
                PlanNode::Project { input: inner, .. } => {
                    // Filter -> Project -> Scan pattern
                    if let PlanNode::Scan { table_name, .. } = inner.as_ref() {
                        hints.insert(
                            table_name.clone(),
                            ScanHint {
                                predicate: Some(predicate.clone()),
                            },
                        );
                    }
                    extract_hints_recursive(inner, hints);
                }
                other => extract_hints_recursive(other, hints),
            }
        }

        PlanNode::Scan { table_name, .. } => {
            // Scan with no parent Filter — insert hint with no predicate
            // (only if not already captured from a parent Filter)
            hints
                .entry(table_name.clone())
                .or_insert(ScanHint { predicate: None });
        }

        PlanNode::Project { input, .. }
        | PlanNode::Aggregate { input, .. }
        | PlanNode::Sort { input, .. }
        | PlanNode::Exchange { input, .. } => {
            extract_hints_recursive(input, hints);
        }

        PlanNode::Join { left, right, .. } => {
            extract_hints_recursive(left, hints);
            extract_hints_recursive(right, hints);
        }
    }
}
