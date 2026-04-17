//! Plan executor — walks a PlanNode tree and executes it against a table registry.
//!
//! Uses rayon for parallel batch processing on filter/project operations.

use std::collections::HashMap;

use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use rayon::prelude::*;

use prism_executor::filter_project;
use prism_executor::hash_aggregate;
use prism_executor::hash_join;
use prism_executor::sort;

use crate::plan::PlanNode;
use crate::{Result, SubstraitError};

fn mem_rss_mb() -> u64 {
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        if let Ok(s) = fs::read_to_string("/proc/self/status") {
            for line in s.lines() {
                if let Some(rest) = line.strip_prefix("VmRSS:") {
                    let parts: Vec<&str> = rest.split_whitespace().collect();
                    if let Some(n) = parts.first() {
                        if let Ok(kb) = n.parse::<u64>() {
                            return kb / 1024;
                        }
                    }
                }
            }
        }
    }
    0
}

/// Execute a PlanNode tree against a table registry (single RecordBatch per table).
pub fn execute_plan(
    node: &PlanNode,
    tables: &HashMap<String, RecordBatch>,
) -> Result<RecordBatch> {
    let chunked: HashMap<String, Vec<RecordBatch>> = tables
        .iter()
        .map(|(k, v)| (k.clone(), vec![v.clone()]))
        .collect();
    execute_plan_chunked(node, &chunked)
}

/// Execute a PlanNode tree against a chunked table registry.
pub fn execute_plan_chunked(
    node: &PlanNode,
    tables: &HashMap<String, Vec<RecordBatch>>,
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
    tables: &HashMap<String, Vec<RecordBatch>>,
) -> Result<Vec<RecordBatch>> {
    match node {
        PlanNode::Scan { table_name, projection, .. } => {
            let batches = tables.get(table_name).ok_or_else(|| {
                SubstraitError::Internal(format!("table '{}' not found in registry", table_name))
            })?;
            if let Some(proj_cols) = projection {
                let mut output = Vec::with_capacity(batches.len());
                for batch in batches {
                    output.push(filter_project::project_batch(batch, proj_cols)?);
                }
                Ok(output)
            } else {
                Ok(batches.clone())
            }
        }

        PlanNode::Filter { input, predicate } => {
            let child_batches = execute_node(input, tables)?;

            // Parallel filter: process batches concurrently with rayon
            let results: Vec<std::result::Result<Option<RecordBatch>, SubstraitError>> = child_batches
                .par_iter()
                .map(|batch| -> std::result::Result<Option<RecordBatch>, SubstraitError> {
                    let mask = filter_project::evaluate_predicate(batch, predicate)
                        .map_err(|e| SubstraitError::Internal(format!("filter error: {}", e)))?;
                    let filtered = filter_project::filter_batch(batch, &mask)
                        .map_err(|e| SubstraitError::Internal(format!("filter error: {}", e)))?;
                    if filtered.num_rows() > 0 {
                        Ok(Some(filtered))
                    } else {
                        Ok(None)
                    }
                })
                .collect();

            let mut output = Vec::new();
            for result in results {
                if let Some(batch) = result? {
                    output.push(batch);
                }
            }
            Ok(output)
        }

        PlanNode::Project { input, columns, expressions } => {
            tracing::info!("MEM pre-project-input: {} MB", mem_rss_mb());
            let child_batches = execute_node(input, tables)?;
            tracing::info!("MEM post-project-input ({} batches): {} MB", child_batches.len(), mem_rss_mb());

            // Parallel project
            let results: Vec<std::result::Result<RecordBatch, SubstraitError>> = child_batches
                .par_iter()
                .map(|batch| -> std::result::Result<RecordBatch, SubstraitError> {
                    if expressions.is_empty() {
                        filter_project::project_batch(batch, columns)
                            .map_err(|e| SubstraitError::Internal(format!("project error: {}", e)))
                    } else {
                        filter_project::project_batch_with_exprs(batch, columns, expressions)
                            .map_err(|e| SubstraitError::Internal(format!("project error: {}", e)))
                    }
                })
                .collect();

            let mut output = Vec::new();
            for result in results {
                output.push(result?);
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

            let config = hash_aggregate::HashAggConfig {
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
            };
            let result = hash_aggregate::hash_aggregate_batches(&child_batches, &config)?;
            Ok(vec![result])
        }

        PlanNode::Join {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
        } => {
            tracing::info!("MEM pre-left-exec: {} MB", mem_rss_mb());
            let left_batches = execute_node(left, tables)?;
            tracing::info!("MEM post-left-exec ({} batches): {} MB", left_batches.len(), mem_rss_mb());
            let right_batches = execute_node(right, tables)?;
            tracing::info!("MEM post-right-exec ({} batches): {} MB", right_batches.len(), mem_rss_mb());

            if left_batches.is_empty() || right_batches.is_empty() {
                return Ok(vec![]);
            }

            let config = hash_join::HashJoinConfig {
                join_type: *join_type,
                probe_keys: left_keys.clone(),
                build_keys: right_keys.clone(),
            };

            // For Inner/Left/Semi/Anti joins: use parallel chunked probing.
            // Build side (right) is typically smaller; concat it once.
            // Probe side (left) stays as multiple batches for parallel processing.
            match join_type {
                hash_join::JoinType::Inner
                | hash_join::JoinType::Left
                | hash_join::JoinType::LeftSemi
                | hash_join::JoinType::LeftAnti => {
                    let right_merged =
                        concat_batches(&right_batches[0].schema(), &right_batches)?;
                    drop(right_batches);
                    tracing::info!("MEM post-right-merge: {} MB", mem_rss_mb());
                    let out = hash_join::hash_join_probe_chunked(&left_batches, &right_merged, &config)?;
                    tracing::info!("MEM post-probe ({} batches): {} MB", out.len(), mem_rss_mb());
                    Ok(out)
                }
                _ => {
                    // Right/Full joins: fall back to single-batch path
                    let left_merged =
                        concat_batches(&left_batches[0].schema(), &left_batches)?;
                    let right_merged =
                        concat_batches(&right_batches[0].schema(), &right_batches)?;
                    let result = hash_join::hash_join(&left_merged, &right_merged, &config)?;
                    Ok(vec![result])
                }
            }
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

            if let Some(lim) = limit {
                let t0 = std::time::Instant::now();
                let total_rows: usize = child_batches.iter().map(|b| b.num_rows()).sum();
                // Fast path: single-column Float64 DESC top-N via heap scan.
                if sort_keys.len() == 1 {
                    if let Some(result) = sort::topn_single_col(&child_batches, &sort_keys[0], *lim)
                        .map_err(|e| SubstraitError::Internal(format!("topn error: {}", e)))?
                    {
                        let t_heap = std::time::Instant::now();
                        tracing::info!(
                            "TOPN_HEAP: rows={} nbatches={} took={:.1}ms",
                            total_rows, child_batches.len(),
                            t_heap.duration_since(t0).as_secs_f64()*1000.0,
                        );
                        return Ok(vec![result]);
                    }
                }
                // Streaming top-N: sort each batch with limit in parallel,
                // then merge-sort the partial results. This avoids concatenating
                // all 150M+ rows just to keep the top N.
                let partial_results: Vec<std::result::Result<RecordBatch, SubstraitError>> = child_batches
                    .par_iter()
                    .map(|batch| {
                        if batch.num_rows() == 0 {
                            return Ok(batch.clone());
                        }
                        sort::sort_batch_limit(batch, sort_keys, *lim)
                            .map_err(|e| SubstraitError::Internal(format!("sort error: {}", e)))
                    })
                    .collect();
                let t1 = std::time::Instant::now();

                let mut sorted_partials: Vec<RecordBatch> = Vec::new();
                for result in partial_results {
                    let batch = result?;
                    if batch.num_rows() > 0 {
                        sorted_partials.push(batch);
                    }
                }

                if sorted_partials.is_empty() {
                    return Ok(vec![]);
                }

                // Merge all partials and do a final sort with limit
                let schema = sorted_partials[0].schema();
                let merged = concat_batches(&schema, &sorted_partials)?;
                let t2 = std::time::Instant::now();
                let result = sort::sort_batch_limit(&merged, sort_keys, *lim)
                    .map_err(|e| SubstraitError::Internal(format!("sort error: {}", e)))?;
                let t3 = std::time::Instant::now();
                tracing::info!(
                    "SORT_PHASES: rows={} nbatches={} partial={:.1}ms merge_sort={:.1}ms total={:.1}ms",
                    total_rows, child_batches.len(),
                    t1.duration_since(t0).as_secs_f64()*1000.0,
                    t3.duration_since(t2).as_secs_f64()*1000.0,
                    t3.duration_since(t0).as_secs_f64()*1000.0,
                );
                Ok(vec![result])
            } else {
                let merged = concat_batches(&child_batches[0].schema(), &child_batches)?;
                let result = sort::sort_batch(&merged, sort_keys)
                    .map_err(|e| SubstraitError::Internal(format!("sort error: {}", e)))?;
                Ok(vec![result])
            }
        }

        PlanNode::Exchange { input, .. } => {
            execute_node(input, tables)
        }
    }
}
