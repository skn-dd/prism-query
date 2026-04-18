//! Plan executor — walks a PlanNode tree and executes it against a table registry.
//!
//! Uses rayon for parallel batch processing on filter/project operations.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::compute;
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
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
            if let Some(result) = try_execute_fused_join_aggregate(input, group_by, aggregates, tables)? {
                return Ok(vec![result]);
            }

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

fn try_execute_fused_join_aggregate(
    input: &PlanNode,
    group_by: &[usize],
    aggregates: &[hash_aggregate::AggExpr],
    tables: &HashMap<String, Vec<RecordBatch>>,
) -> Result<Option<RecordBatch>> {
    let (join, project_columns, project_exprs, fused_group_by, fused_aggs) = match input {
        PlanNode::Project {
            input,
            columns,
            expressions,
        } => match input.as_ref() {
            PlanNode::Join { .. } => (
                input.as_ref(),
                columns.clone(),
                expressions.clone(),
                group_by.to_vec(),
                aggregates.to_vec(),
            ),
            _ => return Ok(None),
        },
        PlanNode::Join { .. } => {
            let projected_columns = fused_identity_projection(group_by, aggregates);
            let col_index: HashMap<usize, usize> = projected_columns
                .iter()
                .enumerate()
                .map(|(new_idx, old_idx)| (*old_idx, new_idx))
                .collect();
            let remapped_group_by = group_by
                .iter()
                .map(|idx| col_index[idx])
                .collect::<Vec<_>>();
            let remapped_aggs = aggregates
                .iter()
                .map(|agg| hash_aggregate::AggExpr {
                    column: col_index[&agg.column],
                    func: agg.func,
                    output_name: agg.output_name.clone(),
                })
                .collect::<Vec<_>>();
            (
                input,
                projected_columns,
                Vec::new(),
                remapped_group_by,
                remapped_aggs,
            )
        }
        _ => return Ok(None),
    };

    let PlanNode::Join {
        left,
        right,
        join_type,
        left_keys,
        right_keys,
    } = join else {
        return Ok(None);
    };

    if *join_type != hash_join::JoinType::Inner {
        return Ok(None);
    }

    let left_batches = execute_node(left, tables)?;
    let right_batches = execute_node(right, tables)?;
    if left_batches.is_empty() || right_batches.is_empty() {
        return Ok(None);
    }

    let right_merged = concat_batches(&right_batches[0].schema(), &right_batches)?;
    let join_config = hash_join::HashJoinConfig {
        join_type: *join_type,
        probe_keys: left_keys.clone(),
        build_keys: right_keys.clone(),
    };

    let (worker_aggs, agg_mapping) = expand_worker_aggregates(&fused_aggs);
    let worker_config = hash_aggregate::HashAggConfig {
        group_by: fused_group_by.clone(),
        aggregates: worker_aggs.clone(),
    };

    let matches = hash_join::probe_matches_chunked(
        &left_batches,
        &right_merged,
        &join_config.probe_keys,
        &join_config.build_keys,
    )?;

    let mut partial_batches = Vec::new();
    for (probe_batch, matched) in left_batches.iter().zip(matches.iter()) {
        if matched.probe_indices.is_empty() {
            continue;
        }
        let projected = project_join_matches(
            probe_batch,
            &matched.probe_indices,
            &right_merged,
            &matched.build_indices,
            &project_columns,
            &project_exprs,
        )?;
        if projected.num_rows() == 0 {
            continue;
        }
        partial_batches.push(hash_aggregate::hash_aggregate(&projected, &worker_config)?);
    }

    if partial_batches.is_empty() {
        return Ok(None);
    }

    let merged_worker_batch = hash_aggregate::hash_aggregate_batches(
        &partial_batches,
        &build_final_merge_config(fused_group_by.len(), &worker_aggs)?,
    )?;
    Ok(Some(reconstruct_original_aggregate_batch(
        &merged_worker_batch,
        fused_group_by.len(),
        aggregates,
        &agg_mapping,
    )?))
}

fn fused_identity_projection(
    group_by: &[usize],
    aggregates: &[hash_aggregate::AggExpr],
) -> Vec<usize> {
    let mut cols = std::collections::BTreeSet::new();
    for idx in group_by {
        cols.insert(*idx);
    }
    for agg in aggregates {
        cols.insert(agg.column);
    }
    cols.into_iter().collect()
}

fn project_join_matches(
    probe_batch: &RecordBatch,
    probe_indices: &arrow_array::UInt32Array,
    build_batch: &RecordBatch,
    build_indices: &arrow_array::UInt32Array,
    columns: &[usize],
    expressions: &[filter_project::ScalarExpr],
) -> Result<RecordBatch> {
    let joined_fields = joined_fields(probe_batch.schema(), build_batch.schema());
    let mut output_fields: Vec<Arc<Field>> = Vec::with_capacity(columns.len() + expressions.len());
    let mut output_columns = Vec::with_capacity(columns.len() + expressions.len());

    for &column in columns {
        output_fields.push(joined_fields[column].clone());
        output_columns.push(take_join_column(
            probe_batch,
            probe_indices,
            build_batch,
            build_indices,
            column,
        )?);
    }

    if !expressions.is_empty() {
        let referenced = collect_expr_input_columns(expressions);
        let temp_fields: Vec<Arc<Field>> = referenced
            .iter()
            .map(|idx| joined_fields[*idx].clone())
            .collect();
        let temp_columns = referenced
            .iter()
            .map(|idx| take_join_column(
                probe_batch,
                probe_indices,
                build_batch,
                build_indices,
                *idx,
            ))
            .collect::<Result<Vec<_>>>()?;
        let temp_schema = SchemaRef::new(Schema::new(temp_fields));
        let temp_batch = RecordBatch::try_new(temp_schema, temp_columns)?;
        let remapped_exprs = expressions
            .iter()
            .map(|expr| remap_expr(expr, &referenced))
            .collect::<Vec<_>>();

        for (expr_idx, expr) in remapped_exprs.iter().enumerate() {
            let array = filter_project::evaluate_scalar_expr(&temp_batch, expr)?;
            output_fields.push(Arc::new(Field::new(
                format!("expr_{}", expr_idx),
                array.data_type().clone(),
                true,
            )));
            output_columns.push(array);
        }
    }

    let schema = SchemaRef::new(Schema::new(output_fields));
    Ok(RecordBatch::try_new(schema, output_columns)?)
}

fn take_join_column(
    probe_batch: &RecordBatch,
    probe_indices: &arrow_array::UInt32Array,
    build_batch: &RecordBatch,
    build_indices: &arrow_array::UInt32Array,
    column: usize,
) -> Result<arrow_array::ArrayRef> {
    if column < probe_batch.num_columns() {
        Ok(compute::take(probe_batch.column(column), probe_indices, None)?)
    } else {
        Ok(compute::take(
            build_batch.column(column - probe_batch.num_columns()),
            build_indices,
            None,
        )?)
    }
}

fn joined_fields(left_schema: SchemaRef, right_schema: SchemaRef) -> Vec<Arc<Field>> {
    let mut fields: Vec<Arc<Field>> = left_schema.fields().iter().cloned().collect();
    for field in right_schema.fields() {
        let mut out = field.as_ref().clone();
        if left_schema.field_with_name(out.name()).is_ok() {
            out = Field::new(
                format!("{}_right", out.name()),
                out.data_type().clone(),
                out.is_nullable(),
            );
        }
        fields.push(Arc::new(out));
    }
    fields
}

fn collect_expr_input_columns(expressions: &[filter_project::ScalarExpr]) -> Vec<usize> {
    let mut cols = std::collections::BTreeSet::new();
    for expr in expressions {
        collect_expr_columns(expr, &mut cols);
    }
    cols.into_iter().collect()
}

fn collect_expr_columns(
    expr: &filter_project::ScalarExpr,
    cols: &mut std::collections::BTreeSet<usize>,
) {
    match expr {
        filter_project::ScalarExpr::ColumnRef(idx) => {
            cols.insert(*idx);
        }
        filter_project::ScalarExpr::Literal(_) => {}
        filter_project::ScalarExpr::BinaryOp { left, right, .. } => {
            collect_expr_columns(left, cols);
            collect_expr_columns(right, cols);
        }
        filter_project::ScalarExpr::Negate(inner) => collect_expr_columns(inner, cols),
    }
}

fn remap_expr(
    expr: &filter_project::ScalarExpr,
    referenced: &[usize],
) -> filter_project::ScalarExpr {
    match expr {
        filter_project::ScalarExpr::ColumnRef(idx) => filter_project::ScalarExpr::ColumnRef(
            referenced
                .iter()
                .position(|col| col == idx)
                .expect("referenced join column missing from remap"),
        ),
        filter_project::ScalarExpr::Literal(v) => filter_project::ScalarExpr::Literal(v.clone()),
        filter_project::ScalarExpr::BinaryOp { op, left, right } => {
            filter_project::ScalarExpr::BinaryOp {
                op: *op,
                left: Box::new(remap_expr(left, referenced)),
                right: Box::new(remap_expr(right, referenced)),
            }
        }
        filter_project::ScalarExpr::Negate(inner) => {
            filter_project::ScalarExpr::Negate(Box::new(remap_expr(inner, referenced)))
        }
    }
}

fn expand_worker_aggregates(
    original: &[hash_aggregate::AggExpr],
) -> (Vec<hash_aggregate::AggExpr>, Vec<Vec<usize>>) {
    let mut worker_aggs = Vec::new();
    let mut agg_mapping = Vec::with_capacity(original.len());

    for agg in original {
        if agg.func == hash_aggregate::AggFunc::Avg {
            let sum_idx = worker_aggs.len();
            worker_aggs.push(hash_aggregate::AggExpr {
                column: agg.column,
                func: hash_aggregate::AggFunc::Sum,
                output_name: format!("agg_{}", sum_idx),
            });
            let count_idx = worker_aggs.len();
            worker_aggs.push(hash_aggregate::AggExpr {
                column: agg.column,
                func: hash_aggregate::AggFunc::Count,
                output_name: format!("agg_{}", count_idx),
            });
            agg_mapping.push(vec![sum_idx, count_idx]);
        } else {
            let idx = worker_aggs.len();
            worker_aggs.push(hash_aggregate::AggExpr {
                column: agg.column,
                func: agg.func,
                output_name: format!("agg_{}", idx),
            });
            agg_mapping.push(vec![idx]);
        }
    }

    (worker_aggs, agg_mapping)
}

fn build_final_merge_config(
    group_by_count: usize,
    worker_aggs: &[hash_aggregate::AggExpr],
) -> Result<hash_aggregate::HashAggConfig> {
    let mut aggregates = Vec::with_capacity(worker_aggs.len());
    for (idx, agg) in worker_aggs.iter().enumerate() {
        let func = match agg.func {
            hash_aggregate::AggFunc::Sum
            | hash_aggregate::AggFunc::Count
            | hash_aggregate::AggFunc::CountDistinct => hash_aggregate::AggFunc::Sum,
            hash_aggregate::AggFunc::Min => hash_aggregate::AggFunc::Min,
            hash_aggregate::AggFunc::Max => hash_aggregate::AggFunc::Max,
            hash_aggregate::AggFunc::Avg => {
                return Err(SubstraitError::Internal(
                    "worker aggregate list must not contain AVG after expansion".into(),
                ));
            }
        };
        aggregates.push(hash_aggregate::AggExpr {
            column: group_by_count + idx,
            func,
            output_name: agg.output_name.clone(),
        });
    }

    Ok(hash_aggregate::HashAggConfig {
        group_by: (0..group_by_count).collect(),
        aggregates,
    })
}

fn reconstruct_original_aggregate_batch(
    merged_worker_batch: &RecordBatch,
    group_by_count: usize,
    original_aggs: &[hash_aggregate::AggExpr],
    agg_mapping: &[Vec<usize>],
) -> Result<RecordBatch> {
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(group_by_count + original_aggs.len());
    let mut columns = Vec::with_capacity(group_by_count + original_aggs.len());

    for idx in 0..group_by_count {
        fields.push(Arc::new(merged_worker_batch.schema().field(idx).clone()));
        columns.push(merged_worker_batch.column(idx).clone());
    }

    for (agg_idx, agg) in original_aggs.iter().enumerate() {
        let source_idx = group_by_count + agg_mapping[agg_idx][0];
        match agg.func {
            hash_aggregate::AggFunc::Count | hash_aggregate::AggFunc::CountDistinct => {
                let values = (0..merged_worker_batch.num_rows())
                    .map(|row| numeric_value_as_i64(merged_worker_batch.column(source_idx), row))
                    .collect::<Result<Vec<_>>>()?;
                fields.push(Arc::new(Field::new(&agg.output_name, arrow_schema::DataType::Int64, true)));
                columns.push(Arc::new(arrow_array::Int64Array::from(values)) as _);
            }
            hash_aggregate::AggFunc::Avg => {
                let count_idx = group_by_count + agg_mapping[agg_idx][1];
                let values = (0..merged_worker_batch.num_rows())
                    .map(|row| {
                        let sum = numeric_value_as_f64(merged_worker_batch.column(source_idx), row)?;
                        let count = numeric_value_as_f64(merged_worker_batch.column(count_idx), row)?;
                        Ok(if count > 0.0 { sum / count } else { 0.0 })
                    })
                    .collect::<Result<Vec<_>>>()?;
                fields.push(Arc::new(Field::new(&agg.output_name, arrow_schema::DataType::Float64, true)));
                columns.push(Arc::new(arrow_array::Float64Array::from(values)) as _);
            }
            hash_aggregate::AggFunc::Sum
            | hash_aggregate::AggFunc::Min
            | hash_aggregate::AggFunc::Max => {
                let values = (0..merged_worker_batch.num_rows())
                    .map(|row| numeric_value_as_f64(merged_worker_batch.column(source_idx), row))
                    .collect::<Result<Vec<_>>>()?;
                fields.push(Arc::new(Field::new(&agg.output_name, arrow_schema::DataType::Float64, true)));
                columns.push(Arc::new(arrow_array::Float64Array::from(values)) as _);
            }
        }
    }

    Ok(RecordBatch::try_new(SchemaRef::new(Schema::new(fields)), columns)?)
}

fn numeric_value_as_f64(array: &arrow_array::ArrayRef, row: usize) -> Result<f64> {
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType;

    if array.is_null(row) {
        return Ok(0.0);
    }
    match array.data_type() {
        DataType::Float64 => Ok(array.as_primitive::<arrow_array::types::Float64Type>().value(row)),
        DataType::Int64 => Ok(array.as_primitive::<arrow_array::types::Int64Type>().value(row) as f64),
        DataType::Int32 => Ok(array.as_primitive::<arrow_array::types::Int32Type>().value(row) as f64),
        other => Err(SubstraitError::Internal(format!(
            "unsupported numeric type for aggregate merge: {:?}",
            other
        ))),
    }
}

fn numeric_value_as_i64(array: &arrow_array::ArrayRef, row: usize) -> Result<i64> {
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType;

    if array.is_null(row) {
        return Ok(0);
    }
    match array.data_type() {
        DataType::Int64 => Ok(array.as_primitive::<arrow_array::types::Int64Type>().value(row)),
        DataType::Int32 => Ok(array.as_primitive::<arrow_array::types::Int32Type>().value(row) as i64),
        DataType::Float64 => Ok(array.as_primitive::<arrow_array::types::Float64Type>().value(row) as i64),
        other => Err(SubstraitError::Internal(format!(
            "unsupported integer type for aggregate merge: {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::cast::AsArray;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::DataType;

    use prism_executor::filter_project::{ArithmeticOp, ScalarExpr, ScalarValue};

    #[test]
    fn fused_join_aggregate_executes_without_materializing_full_join_output() {
        let left_schema = SchemaRef::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
        ]));
        let right_schema = SchemaRef::new(Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
        ]));

        let left_batches = vec![
            RecordBatch::try_new(
                left_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![0, 1])),
                    Arc::new(Float64Array::from(vec![100.0, 200.0])),
                    Arc::new(Float64Array::from(vec![0.10, 0.20])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                left_schema,
                vec![
                    Arc::new(Int64Array::from(vec![2, 3])),
                    Arc::new(Float64Array::from(vec![300.0, 400.0])),
                    Arc::new(Float64Array::from(vec![0.00, 0.25])),
                ],
            )
            .unwrap(),
        ];
        let right_batches = vec![RecordBatch::try_new(
            right_schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
                Arc::new(StringArray::from(vec!["F", "F", "O", "O"])),
            ],
        )
        .unwrap()];

        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Project {
                input: Box::new(PlanNode::Join {
                    left: Box::new(PlanNode::Scan {
                        table_name: "lineitem".into(),
                        schema: left_batches[0].schema(),
                        projection: None,
                    }),
                    right: Box::new(PlanNode::Scan {
                        table_name: "orders".into(),
                        schema: right_batches[0].schema(),
                        projection: None,
                    }),
                    join_type: hash_join::JoinType::Inner,
                    left_keys: vec![0],
                    right_keys: vec![0],
                }),
                columns: vec![4],
                expressions: vec![ScalarExpr::BinaryOp {
                    op: ArithmeticOp::Multiply,
                    left: Box::new(ScalarExpr::ColumnRef(1)),
                    right: Box::new(ScalarExpr::BinaryOp {
                        op: ArithmeticOp::Subtract,
                        left: Box::new(ScalarExpr::Literal(ScalarValue::Float64(1.0))),
                        right: Box::new(ScalarExpr::ColumnRef(2)),
                    }),
                }],
            }),
            group_by: vec![0],
            aggregates: vec![
                hash_aggregate::AggExpr {
                    column: 1,
                    func: hash_aggregate::AggFunc::Sum,
                    output_name: "agg_0".into(),
                },
                hash_aggregate::AggExpr {
                    column: 1,
                    func: hash_aggregate::AggFunc::Count,
                    output_name: "agg_1".into(),
                },
            ],
        };

        let mut tables = HashMap::new();
        tables.insert("lineitem".into(), left_batches);
        tables.insert("orders".into(), right_batches);

        let result = execute_plan_chunked(&plan, &tables).unwrap();
        let statuses = result.column(0).as_string::<i32>();
        let revenue = result.column(1).as_primitive::<arrow_array::types::Float64Type>();
        let counts = result.column(2).as_primitive::<arrow_array::types::Int64Type>();

        let mut rows = HashMap::new();
        for row in 0..result.num_rows() {
            rows.insert(
                statuses.value(row).to_string(),
                (revenue.value(row), counts.value(row)),
            );
        }

        assert_eq!(rows["F"], (100.0 * 0.9 + 200.0 * 0.8, 2));
        assert_eq!(rows["O"], (300.0 + 400.0 * 0.75, 2));
    }

    #[test]
    fn fused_join_avg_aggregate_executes_for_direct_join_input() {
        let left_schema = SchemaRef::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
        ]));
        let right_schema = SchemaRef::new(Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
        ]));

        let lineitem = vec![RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0])),
            ],
        )
        .unwrap()];
        let orders = vec![RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
                Arc::new(StringArray::from(vec!["F", "F", "O", "O"])),
            ],
        )
        .unwrap()];

        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Join {
                left: Box::new(PlanNode::Scan {
                    table_name: "lineitem".into(),
                    schema: left_schema,
                    projection: None,
                }),
                right: Box::new(PlanNode::Scan {
                    table_name: "orders".into(),
                    schema: right_schema,
                    projection: None,
                }),
                join_type: hash_join::JoinType::Inner,
                left_keys: vec![0],
                right_keys: vec![0],
            }),
            group_by: vec![3],
            aggregates: vec![hash_aggregate::AggExpr {
                column: 1,
                func: hash_aggregate::AggFunc::Avg,
                output_name: "agg_0".into(),
            }],
        };

        let mut tables = HashMap::new();
        tables.insert("lineitem".into(), lineitem);
        tables.insert("orders".into(), orders);

        let result = execute_plan_chunked(&plan, &tables).unwrap();
        let statuses = result.column(0).as_string::<i32>();
        let avg = result.column(1).as_primitive::<arrow_array::types::Float64Type>();

        let mut rows = HashMap::new();
        for row in 0..result.num_rows() {
            rows.insert(statuses.value(row).to_string(), avg.value(row));
        }

        assert_eq!(rows["F"], 15.0);
        assert_eq!(rows["O"], 35.0);
    }
}
