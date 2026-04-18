//! Plan optimization — extract scan hints for Parquet pushdown.
//!
//! Walks the plan tree to find Filter predicates and column references
//! adjacent to Scan nodes, returning hints for row group skipping and
//! column pruning during Parquet loading.

use std::collections::{HashMap, HashSet};

use prism_executor::filter_project::Predicate;

use crate::plan::PlanNode;

/// Hints extracted from the plan tree for a specific table scan.
#[derive(Debug, Clone)]
pub struct ScanHint {
    /// Filter predicate to push down to Parquet row group skipping.
    pub predicate: Option<Predicate>,
    /// Column indices actually referenced by the query.
    /// When set, the Parquet reader only reads these columns (column pruning).
    pub projection: Option<Vec<usize>>,
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
                            projection: None,
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
                                projection: None,
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
            hints.entry(table_name.clone()).or_insert(ScanHint {
                predicate: None,
                projection: None,
            });
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

    // After extracting predicates, collect column references for projection
    // This is done in a second pass via extract_projection_hints
}

/// Walk the plan tree and collect which columns each Scan actually needs.
/// This enables column pruning — only reading referenced columns from Parquet.
pub fn extract_projection_hints(node: &PlanNode, hints: &mut HashMap<String, ScanHint>) {
    let mut cols_per_table: HashMap<String, HashSet<usize>> = HashMap::new();

    // Top-down pass: starting from the root with all output cols needed,
    // push column requirements down through each operator. At Join, split
    // requirements between left and right by output offset; always include
    // join keys for each side. At Project, map output references back to
    // input columns (whether bare column refs or expressions). At Scan,
    // record the set of needed columns for that table.
    let root_outputs = output_col_count(node);
    let needed: HashSet<usize> = (0..root_outputs).collect();
    push_columns_down(node, &needed, &mut cols_per_table);

    // Also seed scans that are reachable but didn't receive any pushed cols
    // (e.g., when needed was empty above them) so the entry exists.
    seed_scans(node, &mut cols_per_table);

    for (table, cols) in cols_per_table {
        if cols.is_empty() {
            continue;
        }
        if cols.contains(&usize::MAX) {
            continue;
        }
        if let Some(hint) = hints.get_mut(&table) {
            let mut sorted: Vec<usize> = cols.into_iter().collect();
            sorted.sort();
            hint.projection = Some(sorted);
        }
    }
}

fn seed_scans(node: &PlanNode, out: &mut HashMap<String, HashSet<usize>>) {
    match node {
        PlanNode::Scan { table_name, .. } => {
            out.entry(table_name.clone()).or_default();
        }
        PlanNode::Filter { input, .. }
        | PlanNode::Project { input, .. }
        | PlanNode::Aggregate { input, .. }
        | PlanNode::Sort { input, .. }
        | PlanNode::Exchange { input, .. } => {
            seed_scans(input, out);
        }
        PlanNode::Join { left, right, .. } => {
            seed_scans(left, out);
            seed_scans(right, out);
        }
    }
}

/// Compute the number of output columns produced by a plan node.
fn output_col_count(node: &PlanNode) -> usize {
    match node {
        PlanNode::Scan { schema, .. } => schema.fields().len(),
        PlanNode::Filter { input, .. }
        | PlanNode::Sort { input, .. }
        | PlanNode::Exchange { input, .. } => output_col_count(input),
        PlanNode::Project { columns, expressions, .. } => columns.len() + expressions.len(),
        PlanNode::Aggregate { group_by, aggregates, .. } => group_by.len() + aggregates.len(),
        PlanNode::Join { left, right, .. } => output_col_count(left) + output_col_count(right),
    }
}

/// Top-down pass: propagate column requirements from parent down to each Scan.
fn push_columns_down(
    node: &PlanNode,
    needed: &HashSet<usize>,
    out: &mut HashMap<String, HashSet<usize>>,
) {
    match node {
        PlanNode::Scan { table_name, .. } => {
            let entry = out.entry(table_name.clone()).or_default();
            for &c in needed {
                entry.insert(c);
            }
        }
        PlanNode::Filter { input, predicate } => {
            let mut combined = needed.clone();
            let mut pred_cols = HashSet::new();
            collect_predicate_columns(predicate, &mut pred_cols);
            combined.extend(pred_cols);
            push_columns_down(input, &combined, out);
        }
        PlanNode::Project { input, columns, expressions } => {
            // Project output: [columns...; expression results...]
            // Map needed output indices back to input col references.
            let mut input_needed: HashSet<usize> = HashSet::new();
            let n_cols = columns.len();
            for &out_idx in needed {
                if out_idx < n_cols {
                    input_needed.insert(columns[out_idx]);
                } else {
                    let expr_idx = out_idx - n_cols;
                    if let Some(expr) = expressions.get(expr_idx) {
                        collect_expr_columns(expr, &mut input_needed);
                    }
                }
            }
            push_columns_down(input, &input_needed, out);
        }
        PlanNode::Aggregate { input, group_by, aggregates } => {
            // Aggregate input cols = group_by cols + aggregate input cols.
            let mut input_needed: HashSet<usize> = HashSet::new();
            for &c in group_by {
                input_needed.insert(c);
            }
            for a in aggregates {
                input_needed.insert(a.column);
            }
            push_columns_down(input, &input_needed, out);
        }
        PlanNode::Join { left, right, left_keys, right_keys, .. } => {
            let left_cols = output_col_count(left);
            let mut left_needed: HashSet<usize> = HashSet::new();
            let mut right_needed: HashSet<usize> = HashSet::new();
            for &out_idx in needed {
                if out_idx < left_cols {
                    left_needed.insert(out_idx);
                } else {
                    right_needed.insert(out_idx - left_cols);
                }
            }
            for &k in left_keys {
                left_needed.insert(k);
            }
            for &k in right_keys {
                right_needed.insert(k);
            }
            push_columns_down(left, &left_needed, out);
            push_columns_down(right, &right_needed, out);
        }
        PlanNode::Sort { input, sort_keys, .. } => {
            let mut combined = needed.clone();
            for sk in sort_keys {
                combined.insert(sk.column);
            }
            push_columns_down(input, &combined, out);
        }
        PlanNode::Exchange { input, partition_keys, .. } => {
            let mut combined = needed.clone();
            for &k in partition_keys {
                combined.insert(k);
            }
            push_columns_down(input, &combined, out);
        }
    }
}

/// Recursively collect all column indices referenced for each table scan.
/// Uses `usize::MAX` as a sentinel to mean "all columns needed" (no pruning).
fn collect_referenced_columns(node: &PlanNode, cols: &mut HashMap<String, HashSet<usize>>) {
    match node {
        PlanNode::Scan {
            table_name,
            projection,
            ..
        } => {
            // Only add columns if the Scan already carries an explicit projection
            // from the Substrait plan. Otherwise, let parent nodes (Project, Filter,
            // Aggregate, etc.) determine which columns are actually needed.
            if let Some(proj) = projection {
                let entry = cols.entry(table_name.clone()).or_default();
                for &col in proj {
                    entry.insert(col);
                }
            }
            // Ensure the table entry exists so extract_projection_hints can find it
            cols.entry(table_name.clone()).or_default();
        }

        PlanNode::Filter { input, predicate } => {
            // Collect columns referenced by the predicate
            let scan_table = find_scan_table(input);
            if let Some(table) = scan_table {
                let entry = cols.entry(table).or_default();
                collect_predicate_columns(predicate, entry);
            }
            collect_referenced_columns(input, cols);
        }

        PlanNode::Project {
            input,
            columns,
            expressions,
        } => {
            let scan_table = find_scan_table(input);
            if let Some(table) = scan_table {
                let entry = cols.entry(table).or_default();
                for &col in columns {
                    entry.insert(col);
                }
                for expr in expressions {
                    collect_expr_columns(expr, entry);
                }
            }
            collect_referenced_columns(input, cols);
        }

        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let scan_table = find_scan_table(input);
            if let Some(table) = scan_table {
                let entry = cols.entry(table).or_default();
                for &col in group_by {
                    entry.insert(col);
                }
                for agg in aggregates {
                    entry.insert(agg.column);
                }
            }
            collect_referenced_columns(input, cols);
        }

        PlanNode::Sort {
            input, sort_keys, ..
        } => {
            // Sort passes through ALL input columns. If the input is a Scan with
            // no projection, we need ALL columns (mark with usize::MAX sentinel).
            // Only add sort key columns if there's already a column-limiting node
            // (Project, Aggregate) below us that determines the column set.
            let scan_table = find_scan_table(input);
            if let Some(ref table) = scan_table {
                if has_column_limiting_node(input) {
                    let entry = cols.entry(table.clone()).or_default();
                    for sk in sort_keys {
                        entry.insert(sk.column);
                    }
                } else {
                    // No column-limiting node below Sort — need all columns
                    cols.entry(table.clone()).or_default().insert(usize::MAX);
                }
            }
            collect_referenced_columns(input, cols);
        }

        PlanNode::Join {
            left,
            right,
            ..
        } => {
            // If a join subtree has column-limiting nodes (Project, Filter, Aggregate),
            // let the recursion determine needed columns from those nodes.
            // Otherwise, fall back to loading all columns (usize::MAX sentinel).
            if !has_column_limiting_node(left) {
                if let Some(table) = find_scan_table(left) {
                    cols.entry(table).or_default().insert(usize::MAX);
                }
            }
            if !has_column_limiting_node(right) {
                if let Some(table) = find_scan_table(right) {
                    cols.entry(table).or_default().insert(usize::MAX);
                }
            }
            collect_referenced_columns(left, cols);
            collect_referenced_columns(right, cols);
        }

        PlanNode::Exchange { input, .. } => {
            collect_referenced_columns(input, cols);
        }
    }
}

/// Check if there is a column-limiting node (Project, Aggregate, Filter) between
/// this node and the underlying Scan. If so, the operator determines which columns
/// are needed. If not, the Scan outputs all columns and they all pass through.
fn has_column_limiting_node(node: &PlanNode) -> bool {
    match node {
        PlanNode::Project { .. } | PlanNode::Aggregate { .. } | PlanNode::Filter { .. } => true,
        PlanNode::Sort { input, .. } | PlanNode::Exchange { input, .. } => {
            has_column_limiting_node(input)
        }
        PlanNode::Scan { .. } | PlanNode::Join { .. } => false,
    }
}

/// Find the table name of the nearest Scan in a subtree.
fn find_scan_table(node: &PlanNode) -> Option<String> {
    match node {
        PlanNode::Scan { table_name, .. } => Some(table_name.clone()),
        PlanNode::Filter { input, .. }
        | PlanNode::Project { input, .. }
        | PlanNode::Aggregate { input, .. }
        | PlanNode::Sort { input, .. }
        | PlanNode::Exchange { input, .. } => find_scan_table(input),
        PlanNode::Join { .. } => None, // ambiguous — skip
    }
}

/// Collect column indices referenced by a predicate.
fn collect_predicate_columns(pred: &Predicate, cols: &mut HashSet<usize>) {
    use Predicate::*;
    match pred {
        Eq(c, _) | Ne(c, _) | Lt(c, _) | Le(c, _) | Gt(c, _) | Ge(c, _) => {
            cols.insert(*c);
        }
        IsNull(c) | IsNotNull(c) => {
            cols.insert(*c);
        }
        Like(c, _) | ILike(c, _) => {
            cols.insert(*c);
        }
        And(l, r) | Or(l, r) => {
            collect_predicate_columns(l, cols);
            collect_predicate_columns(r, cols);
        }
        Not(inner) => {
            collect_predicate_columns(inner, cols);
        }
    }
}

/// Detect if a plan is a simple global COUNT(*) on a single scan, optionally with a filter.
///
/// Returns `Some((table_name, optional_predicate))` if the plan matches:
///   - Aggregate(Scan) with group_by=[], single Count aggregate
///   - Aggregate(Filter(Scan)) with group_by=[], single Count aggregate
///
/// This allows the handler to short-circuit using Parquet metadata-only row counts.
pub fn detect_count_star(node: &PlanNode) -> Option<(String, Option<Predicate>)> {
    use prism_executor::hash_aggregate::AggFunc;

    if let PlanNode::Aggregate { input, group_by, aggregates } = node {
        // Must be a global aggregate (no GROUP BY)
        if !group_by.is_empty() {
            return None;
        }
        // All aggregates must be Count (covers SELECT COUNT(*), COUNT(*), ...)
        if aggregates.is_empty() || !aggregates.iter().all(|a| a.func == AggFunc::Count) {
            return None;
        }
        // Check if the input is a Scan, Filter(Scan), or with intervening Project nodes
        let inner = skip_projects(input.as_ref());
        match inner {
            PlanNode::Scan { table_name, .. } => {
                return Some((table_name.clone(), None));
            }
            PlanNode::Filter { input: filter_inner, predicate } => {
                let scan_node = skip_projects(filter_inner.as_ref());
                if let PlanNode::Scan { table_name, .. } = scan_node {
                    return Some((table_name.clone(), Some(predicate.clone())));
                }
            }
            _ => {}
        }
    }
    None
}

/// Skip through Project nodes to reach the underlying node.
fn skip_projects(node: &PlanNode) -> &PlanNode {
    match node {
        PlanNode::Project { input, .. } => skip_projects(input.as_ref()),
        other => other,
    }
}

/// Collect column indices referenced by a scalar expression.
fn collect_expr_columns(
    expr: &prism_executor::filter_project::ScalarExpr,
    cols: &mut HashSet<usize>,
) {
    use prism_executor::filter_project::ScalarExpr::*;
    match expr {
        ColumnRef(c) => {
            cols.insert(*c);
        }
        Literal(_) => {}
        BinaryOp { left, right, .. } => {
            collect_expr_columns(left, cols);
            collect_expr_columns(right, cols);
        }
        Negate(inner) => {
            collect_expr_columns(inner, cols);
        }
    }
}
