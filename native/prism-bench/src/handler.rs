//! Query execution handler — implements the ActionHandler trait
//! to execute TPC-H queries on local data stored in PartitionStore.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::FlightDescriptor;
use arrow_schema::{DataType, Field, Schema};
use base64::Engine;
use futures::{stream, TryStreamExt};
use serde::Deserialize;
use tonic::transport::Channel;

use prism_executor::hash_aggregate::{AggExpr, AggFunc, HashAggConfig};
use prism_executor::parquet_scan::{ParquetScanConfig, parquet_scan, parquet_row_count};
use prism_flight::shuffle_writer::{ActionHandler, PartitionStore};
use prism_substrait::plan::PlanNode;
use prism_substrait::plan_opt::{ScanHint, extract_scan_hints, extract_projection_hints, detect_count_star};

use crate::{datagen, queries};

/// Handles "execute" DoAction commands from the coordinator.
pub struct QueryHandler {
    /// Directory for Parquet data files. When set, the handler checks
    /// for Parquet files at {data_dir}/{store_key}/ before falling back
    /// to the in-memory PartitionStore.
    data_dir: Option<PathBuf>,
}

impl QueryHandler {
    pub fn new() -> Self {
        Self { data_dir: None }
    }

    pub fn with_data_dir(data_dir: PathBuf) -> Self {
        Self {
            data_dir: Some(data_dir),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct DistributedAggregateCommand {
    query_id: String,
    role: String,
    worker_index: usize,
    reducer_index: usize,
    reducer_endpoint: String,
    expected_workers: usize,
    timeout_ms: Option<u64>,
}

impl DistributedAggregateCommand {
    fn is_reducer(&self) -> bool {
        self.role == "reducer"
    }

    fn partial_key(&self, worker_index: usize) -> String {
        format!("__partialagg/{}/{}", self.query_id, worker_index)
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms.unwrap_or(30_000))
    }
}

#[tonic::async_trait]
impl ActionHandler for QueryHandler {
    async fn execute(
        &self,
        command: serde_json::Value,
        store: &PartitionStore,
    ) -> anyhow::Result<RecordBatch> {
        // --- Substrait plan dispatch (takes priority over hardcoded queries) ---
        if let Some(plan_b64) = command.get("substrait_plan_b64").and_then(|v| v.as_str()) {
            let plan_bytes = base64::engine::general_purpose::STANDARD
                .decode(plan_b64)
                .map_err(|e| anyhow::anyhow!("base64 decode error: {}", e))?;

            let start = Instant::now();
            let plan = prism_substrait::consumer::consume_plan(&plan_bytes)
                .map_err(|e| anyhow::anyhow!("Substrait consume error: {}", e))?;

            // Extract scan hints for Parquet row group skipping + column pruning
            let mut hints = extract_scan_hints(&plan.root);
            extract_projection_hints(&plan.root, &mut hints);

            tracing::debug!("executing plan: {:#?}", plan.root);
            for (t, h) in &hints {
                tracing::debug!(
                    "scan hint: table={} projection={:?} predicate={:?}",
                    t,
                    h.projection,
                    h.predicate
                );
            }

            // Fast path: unfiltered COUNT(*) from Parquet metadata only (no data reading).
            // Only used when there is NO predicate — filtered COUNT(*) must go through
            // the executor for exact results (row-group skipping only gives an overestimate).
            if let Some((table_name, pred)) = detect_count_star(&plan.root) {
                if pred.is_none() {
                    if let Some(spec) = command["tables"]
                        .as_object()
                        .and_then(|m| m.get(&table_name))
                    {
                        let uris = resolve_table_uris(spec, self.data_dir.as_deref());
                        if !uris.is_empty() {
                            let count = parquet_row_count(uris, None)
                                .await
                                .map_err(|e| anyhow::anyhow!("Parquet row count error: {}", e))?;

                            tracing::info!(
                                "COUNT(*) metadata shortcut for '{}': {} rows in {:.2}ms",
                                table_name,
                                count,
                                start.elapsed().as_secs_f64() * 1000.0,
                            );

                            let schema = Arc::new(Schema::new(vec![
                                Field::new(&plan.root.aggregate_output_name().unwrap_or("cnt".into()),
                                           DataType::Int64, true),
                            ]));
                            return Ok(RecordBatch::try_new(
                                schema,
                                vec![Arc::new(Int64Array::from(vec![count as i64]))],
                            )?);
                        }
                    }
                }
            }

            // Load tables — Parquet with pushdown if available, else in-memory
            let (tables, col_remaps) = load_tables_smart(&command, store, self.data_dir.as_deref(), &hints).await?;
            for (t, batches) in &tables {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                let cols = batches.first().map(|b| b.num_columns()).unwrap_or(0);
                let schema_str = batches
                    .first()
                    .map(|b| {
                        format!(
                            "{:?}",
                            b.schema()
                                .fields()
                                .iter()
                                .map(|f| f.name().clone())
                                .collect::<Vec<_>>()
                        )
                    })
                    .unwrap_or_default();
                tracing::debug!(
                    "loaded table={} rows={} cols={} nbatches={} schema={}",
                    t,
                    rows,
                    cols,
                    batches.len(),
                    schema_str
                );
            }

            // Remap column indices for projected Parquet loading with skip_expand.
            // Handles single-table (trivial) and multi-table JOIN cases (per-subtree remap).
            let mut plan_root = plan.root.clone();
            if !col_remaps.is_empty() {
                remap_plan_multi_table(&mut plan_root, &col_remaps);
            }

            let distributed = command
                .get("distributed_aggregate")
                .cloned()
                .map(serde_json::from_value::<DistributedAggregateCommand>)
                .transpose()
                .map_err(|e| anyhow::anyhow!("distributed aggregate command error: {}", e))?;

            let result = if let Some(dist) = distributed.as_ref() {
                execute_distributed_aggregate(&plan_root, &tables, store, dist).await?
            } else {
                prism_substrait::executor::execute_plan_chunked(&plan_root, &tables)
                    .map_err(|e| anyhow::anyhow!("Substrait execute error: {}", e))?
            };
            let elapsed = start.elapsed();

            let total_rows: usize = tables
                .values()
                .map(|bs| bs.iter().map(|b| b.num_rows()).sum::<usize>())
                .sum();
            tracing::info!(
                "Executed Substrait plan on {} tables ({} total rows) -> {} rows in {:.2}ms",
                tables.len(),
                total_rows,
                result.num_rows(),
                elapsed.as_secs_f64() * 1000.0,
            );

            // Explicitly drop loaded tables to free Arrow buffers, then
            // ask glibc to return freed pages to the OS. Without this,
            // the allocator holds onto pages across queries, causing OOM
            // at large scale factors.
            drop(tables);
            #[cfg(target_os = "linux")]
            unsafe { libc::malloc_trim(0); }

            return Ok(result);
        }

        let query_name = command["query"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing 'query' field"))?;

        // --- Parquet data generation action ---
        if query_name == "datagen_parquet" {
            return self.handle_datagen_parquet(&command).await;
        }

        // --- In-memory data generation action ---
        if query_name == "datagen" {
            return self.handle_datagen(&command, store).await;
        }

        // --- Hardcoded query dispatch (legacy benchmark path) ---
        let tables = load_tables(&command, store).await?;

        let start = Instant::now();
        let result = match query_name {
            "q1" => queries::execute_q1(&tables)?,
            "q3" => queries::execute_q3(&tables)?,
            "q6" => queries::execute_q6(&tables)?,
            "scan" => tables
                .get("lineitem")
                .ok_or_else(|| anyhow::anyhow!("lineitem table not found"))?
                .clone(),
            other => return Err(anyhow::anyhow!("unknown query: {}", other)),
        };
        let elapsed = start.elapsed();

        tracing::info!(
            "Executed {} on {} tables -> {} rows in {:.2}ms",
            query_name,
            tables.len(),
            result.num_rows(),
            elapsed.as_secs_f64() * 1000.0,
        );

        Ok(result)
    }
}

async fn execute_distributed_aggregate(
    plan_root: &PlanNode,
    tables: &HashMap<String, Vec<RecordBatch>>,
    store: &PartitionStore,
    command: &DistributedAggregateCommand,
) -> anyhow::Result<RecordBatch> {
    let (group_by, original_aggs) = find_aggregate(plan_root)
        .ok_or_else(|| anyhow::anyhow!("distributed aggregate requested for non-aggregate plan"))?;
    let (worker_aggs, agg_mapping) = expand_worker_aggregates(&original_aggs);
    let worker_plan = replace_aggregates(plan_root, &worker_aggs)?;

    let local_partial = prism_substrait::executor::execute_plan_chunked(&worker_plan, tables)
        .map_err(|e| anyhow::anyhow!("Substrait distributed partial execute error: {}", e))?;

    if !command.is_reducer() {
        let partial_rows = local_partial.num_rows();
        let partial_bytes: usize = local_partial
            .columns()
            .iter()
            .map(|col| col.get_array_memory_size())
            .sum();
        let send_start = Instant::now();
        tracing::info!(
            "distributed aggregate producer {} sending {} rows ({} bytes) to reducer {}",
            command.worker_index,
            partial_rows,
            partial_bytes,
            command.reducer_index,
        );
        send_partial_batch(
            &command.reducer_endpoint,
            &command.partial_key(command.worker_index),
            local_partial.clone(),
        )
        .await?;
        tracing::info!(
            "distributed aggregate producer {} sent partial batch in {:.2}ms",
            command.worker_index,
            send_start.elapsed().as_secs_f64() * 1000.0,
        );
        return Ok(RecordBatch::new_empty(local_partial.schema()));
    }

    let peer_keys: Vec<String> = (0..command.expected_workers)
        .filter(|idx| *idx != command.worker_index)
        .map(|idx| command.partial_key(idx))
        .collect();
    let mut partial_batches = vec![local_partial];
    let wait_start = Instant::now();
    partial_batches.extend(wait_for_partial_batches(store, &peer_keys, command.timeout()).await?);
    tracing::info!(
        "distributed aggregate reducer {} waited {:.2}ms for {} peer partial batch(es)",
        command.worker_index,
        wait_start.elapsed().as_secs_f64() * 1000.0,
        partial_batches.len().saturating_sub(1),
    );

    let merge_config = build_final_merge_config(group_by.len(), &worker_aggs)?;
    let merged_worker_batch = prism_executor::hash_aggregate::hash_aggregate_batches(
        &partial_batches,
        &merge_config,
    )?;

    tracing::info!(
        "distributed aggregate reducer {} merged {} partial batches into {} rows",
        command.worker_index,
        partial_batches.len(),
        merged_worker_batch.num_rows(),
    );

    reconstruct_original_aggregate_batch(
        &merged_worker_batch,
        group_by.len(),
        &original_aggs,
        &agg_mapping,
    )
}

fn find_aggregate(node: &PlanNode) -> Option<(Vec<usize>, Vec<AggExpr>)> {
    match node {
        PlanNode::Aggregate {
            group_by,
            aggregates,
            ..
        } => Some((group_by.clone(), aggregates.clone())),
        PlanNode::Filter { input, .. }
        | PlanNode::Project { input, .. }
        | PlanNode::Sort { input, .. } => find_aggregate(input),
        _ => None,
    }
}

fn replace_aggregates(node: &PlanNode, new_aggs: &[AggExpr]) -> anyhow::Result<PlanNode> {
    match node {
        PlanNode::Aggregate { input, group_by, .. } => Ok(PlanNode::Aggregate {
            input: input.clone(),
            group_by: group_by.clone(),
            aggregates: new_aggs.to_vec(),
        }),
        PlanNode::Filter { input, predicate } => Ok(PlanNode::Filter {
            input: Box::new(replace_aggregates(input, new_aggs)?),
            predicate: predicate.clone(),
        }),
        PlanNode::Project {
            input,
            columns,
            expressions,
        } => Ok(PlanNode::Project {
            input: Box::new(replace_aggregates(input, new_aggs)?),
            columns: columns.clone(),
            expressions: expressions.clone(),
        }),
        PlanNode::Sort {
            input,
            sort_keys,
            limit,
        } => Ok(PlanNode::Sort {
            input: Box::new(replace_aggregates(input, new_aggs)?),
            sort_keys: sort_keys.clone(),
            limit: *limit,
        }),
        _ => Err(anyhow::anyhow!(
            "distributed aggregate expects Aggregate at root or below filter/project/sort"
        )),
    }
}

fn expand_worker_aggregates(original: &[AggExpr]) -> (Vec<AggExpr>, Vec<Vec<usize>>) {
    let mut worker_aggs = Vec::new();
    let mut agg_mapping = Vec::with_capacity(original.len());

    for agg in original {
        if agg.func == AggFunc::Avg {
            let sum_idx = worker_aggs.len();
            worker_aggs.push(AggExpr {
                column: agg.column,
                func: AggFunc::Sum,
                output_name: format!("agg_{}", sum_idx),
            });
            let count_idx = worker_aggs.len();
            worker_aggs.push(AggExpr {
                column: agg.column,
                func: AggFunc::Count,
                output_name: format!("agg_{}", count_idx),
            });
            agg_mapping.push(vec![sum_idx, count_idx]);
        } else {
            let idx = worker_aggs.len();
            worker_aggs.push(AggExpr {
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
    worker_aggs: &[AggExpr],
) -> anyhow::Result<HashAggConfig> {
    let mut aggregates = Vec::with_capacity(worker_aggs.len());
    for (idx, agg) in worker_aggs.iter().enumerate() {
        let func = match agg.func {
            AggFunc::Sum | AggFunc::Count | AggFunc::CountDistinct => AggFunc::Sum,
            AggFunc::Min => AggFunc::Min,
            AggFunc::Max => AggFunc::Max,
            AggFunc::Avg => {
                return Err(anyhow::anyhow!(
                    "worker aggregate list must not contain AVG after expansion"
                ));
            }
        };
        aggregates.push(AggExpr {
            column: group_by_count + idx,
            func,
            output_name: agg.output_name.clone(),
        });
    }

    Ok(HashAggConfig {
        group_by: (0..group_by_count).collect(),
        aggregates,
    })
}

fn reconstruct_original_aggregate_batch(
    merged_worker_batch: &RecordBatch,
    group_by_count: usize,
    original_aggs: &[AggExpr],
    agg_mapping: &[Vec<usize>],
) -> anyhow::Result<RecordBatch> {
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(group_by_count + original_aggs.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(group_by_count + original_aggs.len());

    for idx in 0..group_by_count {
        fields.push(Arc::new(merged_worker_batch.schema().field(idx).clone()));
        columns.push(merged_worker_batch.column(idx).clone());
    }

    for (agg_idx, agg) in original_aggs.iter().enumerate() {
        let source_idx = group_by_count + agg_mapping[agg_idx][0];
        match agg.func {
            AggFunc::Count | AggFunc::CountDistinct => {
                let mut values = Vec::with_capacity(merged_worker_batch.num_rows());
                for row in 0..merged_worker_batch.num_rows() {
                    values.push(numeric_value_as_i64(merged_worker_batch.column(source_idx), row)?);
                }
                fields.push(Arc::new(Field::new(&agg.output_name, DataType::Int64, true)));
                columns.push(Arc::new(Int64Array::from(values)));
            }
            AggFunc::Avg => {
                let count_idx = group_by_count + agg_mapping[agg_idx][1];
                let mut values = Vec::with_capacity(merged_worker_batch.num_rows());
                for row in 0..merged_worker_batch.num_rows() {
                    let sum = numeric_value_as_f64(merged_worker_batch.column(source_idx), row)?;
                    let count = numeric_value_as_f64(merged_worker_batch.column(count_idx), row)?;
                    values.push(if count > 0.0 { sum / count } else { 0.0 });
                }
                fields.push(Arc::new(Field::new(&agg.output_name, DataType::Float64, true)));
                columns.push(Arc::new(Float64Array::from(values)));
            }
            AggFunc::Sum | AggFunc::Min | AggFunc::Max => {
                let mut values = Vec::with_capacity(merged_worker_batch.num_rows());
                for row in 0..merged_worker_batch.num_rows() {
                    values.push(numeric_value_as_f64(merged_worker_batch.column(source_idx), row)?);
                }
                fields.push(Arc::new(Field::new(&agg.output_name, DataType::Float64, true)));
                columns.push(Arc::new(Float64Array::from(values)));
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn numeric_value_as_f64(array: &ArrayRef, row: usize) -> anyhow::Result<f64> {
    if array.is_null(row) {
        return Ok(0.0);
    }
    match array.data_type() {
        DataType::Float64 => Ok(array.as_primitive::<arrow_array::types::Float64Type>().value(row)),
        DataType::Int64 => Ok(array.as_primitive::<arrow_array::types::Int64Type>().value(row) as f64),
        DataType::Int32 => Ok(array.as_primitive::<arrow_array::types::Int32Type>().value(row) as f64),
        other => Err(anyhow::anyhow!(
            "unsupported numeric type for distributed aggregate merge: {:?}",
            other
        )),
    }
}

fn numeric_value_as_i64(array: &ArrayRef, row: usize) -> anyhow::Result<i64> {
    if array.is_null(row) {
        return Ok(0);
    }
    match array.data_type() {
        DataType::Int64 => Ok(array.as_primitive::<arrow_array::types::Int64Type>().value(row)),
        DataType::Int32 => Ok(array.as_primitive::<arrow_array::types::Int32Type>().value(row) as i64),
        DataType::Float64 => Ok(array.as_primitive::<arrow_array::types::Float64Type>().value(row) as i64),
        other => Err(anyhow::anyhow!(
            "unsupported integer type for distributed aggregate merge: {:?}",
            other
        )),
    }
}

async fn send_partial_batch(
    endpoint: &str,
    key: &str,
    batch: RecordBatch,
) -> anyhow::Result<()> {
    let attempts = 3;
    let batch_rows = batch.num_rows();
    let batch_bytes: usize = batch
        .columns()
        .iter()
        .map(|col| col.get_array_memory_size())
        .sum();

    for attempt in 1..=attempts {
        let channel = Channel::from_shared(format!("http://{}", endpoint))?
            .connect()
            .await?;
        let mut client = FlightServiceClient::new(channel);
        let flight_data: Vec<_> = FlightDataEncoderBuilder::new()
            .with_schema(batch.schema())
            .with_flight_descriptor(Some(FlightDescriptor::new_cmd(key.as_bytes().to_vec())))
            .build(stream::iter(vec![Ok(batch.clone())]))
            .try_collect()
            .await
            .map_err(|e| anyhow::anyhow!("failed to encode partial aggregate batch: {}", e))?;

        match client.do_put(stream::iter(flight_data)).await {
            Ok(response) => {
                let mut response = response.into_inner();
                while response.message().await?.is_some() {}
                return Ok(());
            }
            Err(err) if attempt < attempts => {
                tracing::warn!(
                    "partial aggregate send attempt {}/{} failed for key '{}' (rows={}, bytes={}): {}",
                    attempt,
                    attempts,
                    key,
                    batch_rows,
                    batch_bytes,
                    err,
                );
                tokio::time::sleep(Duration::from_millis(100 * attempt as u64)).await;
            }
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "partial aggregate send failed after {} attempts for key '{}' (rows={}, bytes={}): {}",
                    attempts,
                    key,
                    batch_rows,
                    batch_bytes,
                    err
                ));
            }
        }
    }

    unreachable!("send_partial_batch must return or error within retry loop")
}

async fn wait_for_partial_batches(
    store: &PartitionStore,
    keys: &[String],
    timeout: Duration,
) -> anyhow::Result<Vec<RecordBatch>> {
    let deadline = Instant::now() + timeout;
    let mut pending = keys.to_vec();
    let mut batches = Vec::new();

    while !pending.is_empty() {
        let mut next_pending = Vec::new();
        for key in pending {
            let stored = store.get(&key).await;
            if stored.is_empty() {
                next_pending.push(key);
                continue;
            }
            store.clear(&key).await;
            batches.extend(stored);
        }

        if next_pending.is_empty() {
            return Ok(batches);
        }
        if Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "timed out waiting for partial aggregate batches: {:?}",
                next_pending
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        pending = next_pending;
    }

    Ok(batches)
}

impl QueryHandler {
    async fn handle_datagen(
        &self,
        command: &serde_json::Value,
        store: &PartitionStore,
    ) -> anyhow::Result<RecordBatch> {
        let table = command["table"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("datagen: missing 'table' field"))?;
        let sf = command["sf"].as_f64().unwrap_or(1.0);
        let store_key = command["store_key"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("datagen: missing 'store_key' field"))?;
        let chunk_size = command["chunk_size"].as_u64().unwrap_or(5_000_000) as usize;
        let row_offset = command["row_offset"].as_u64().unwrap_or(0) as usize;
        let row_count = command["row_count"].as_u64().map(|v| v as usize);

        let start = Instant::now();
        let batches = match table {
            "lineitem" => {
                let n = row_count.unwrap_or((6_000_000.0 * sf) as usize);
                datagen::make_lineitem_shard(n, row_offset, chunk_size)
            }
            "orders" => {
                let n = row_count.unwrap_or((1_500_000.0 * sf) as usize);
                datagen::make_orders_shard(n, row_offset, chunk_size)
            }
            other => return Err(anyhow::anyhow!("datagen: unknown table '{}'", other)),
        };
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        store.clear(store_key).await;
        for batch in batches {
            store.put(store_key, batch).await;
        }
        let elapsed = start.elapsed();

        tracing::info!(
            "Generated {} ({} rows, SF={}) stored as '{}' in {:.0}ms",
            table,
            rows,
            sf,
            store_key,
            elapsed.as_secs_f64() * 1000.0,
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("table", DataType::Utf8, false),
            Field::new("rows", DataType::Int64, false),
            Field::new("store_key", DataType::Utf8, false),
        ]));
        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![table])),
                Arc::new(Int64Array::from(vec![rows as i64])),
                Arc::new(StringArray::from(vec![store_key])),
            ],
        )?)
    }

    async fn handle_datagen_parquet(
        &self,
        command: &serde_json::Value,
    ) -> anyhow::Result<RecordBatch> {
        let table = command["table"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("datagen_parquet: missing 'table' field"))?;
        let store_key = command["store_key"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("datagen_parquet: missing 'store_key'"))?;
        let row_count = command["row_count"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("datagen_parquet: missing 'row_count'"))?
            as usize;
        let row_offset = command["row_offset"].as_u64().unwrap_or(0) as usize;
        let row_group_size = command["row_group_size"].as_u64().unwrap_or(1_000_000) as usize;

        let data_dir = self
            .data_dir
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("datagen_parquet: --data-dir not set"))?;
        let output_dir = data_dir.join(store_key);

        let start = Instant::now();
        let file_path = match table {
            "lineitem" => {
                datagen::write_lineitem_parquet(&output_dir, row_count, row_offset, row_group_size)?
            }
            "orders" => {
                datagen::write_orders_parquet(&output_dir, row_count, row_offset, row_group_size)?
            }
            other => return Err(anyhow::anyhow!("datagen_parquet: unknown table '{}'", other)),
        };
        let elapsed = start.elapsed();

        tracing::info!(
            "Generated Parquet {} ({} rows) at {:?} in {:.0}ms",
            table,
            row_count,
            file_path,
            elapsed.as_secs_f64() * 1000.0,
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("table", DataType::Utf8, false),
            Field::new("rows", DataType::Int64, false),
            Field::new("path", DataType::Utf8, false),
        ]));
        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![table])),
                Arc::new(Int64Array::from(vec![row_count as i64])),
                Arc::new(StringArray::from(vec![file_path.to_string_lossy().to_string()])),
            ],
        )?)
    }
}

/// Table specification as delivered in the v2 DoAction protocol.
/// Each entry in `tables` is an object: `{ "uris": [...], "store_key": "..." }`.
/// - `uris` (required, may be empty): fully-qualified Parquet file or directory
///   URIs (`file://`, `s3://`, `gs://`, `abfs[s]://`, or bare local paths).
/// - `store_key` (optional): legacy fallback used when `uris` is empty — the
///   worker resolves the table either from the `{--data-dir}/{store_key}/`
///   Parquet tree (bench scenarios) or from the in-memory `PartitionStore`
///   (DoPut-pushed tables from tests).
///
/// We deliberately do NOT support the old bare-string form; the only in-tree
/// caller is Prism itself and the brief requires a clean break.
#[derive(Debug, Default)]
struct TableSpec {
    uris: Vec<String>,
    store_key: Option<String>,
}

fn parse_table_spec(name: &str, v: &serde_json::Value) -> anyhow::Result<TableSpec> {
    let obj = v.as_object().ok_or_else(|| {
        anyhow::anyhow!(
            "table '{}' must be an object of the form {{\"uris\": [...], \"store_key\": \"...\"}}",
            name
        )
    })?;

    let uris = match obj.get("uris") {
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .map(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .ok_or_else(|| anyhow::anyhow!("table '{}' uris must be strings", name))
            })
            .collect::<anyhow::Result<Vec<String>>>()?,
        Some(_) => {
            return Err(anyhow::anyhow!(
                "table '{}' 'uris' field must be a JSON array",
                name
            ))
        }
        None => Vec::new(),
    };
    let store_key = obj
        .get("store_key")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    if uris.is_empty() && store_key.is_none() {
        return Err(anyhow::anyhow!(
            "table '{}' must have either non-empty 'uris' or a 'store_key'",
            name
        ));
    }
    Ok(TableSpec { uris, store_key })
}

/// Resolve a table spec to the URI list we will scan. Used by the COUNT(*)
/// metadata shortcut. Returns URIs derived from either the explicit `uris`
/// list or — for the bench legacy path — `data_dir.join(store_key)`.
fn resolve_table_uris(v: &serde_json::Value, data_dir: Option<&Path>) -> Vec<String> {
    let spec = match parse_table_spec("<count-star>", v) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    if !spec.uris.is_empty() {
        return spec.uris;
    }
    if let (Some(dir), Some(key)) = (data_dir, spec.store_key.as_deref()) {
        let path = dir.join(key);
        if path.exists() {
            return vec![path.to_string_lossy().into_owned()];
        }
    }
    Vec::new()
}

/// Load tables with Parquet support. Uses the v2 per-table spec
/// `{ uris: [...], store_key?: "..." }`. When `uris` is non-empty, every URI
/// is scanned as Parquet. Otherwise we fall back to `{data_dir}/{store_key}/`
/// or the in-memory `PartitionStore`.
///
/// Returns the tables AND column remap mappings (orig_col_idx -> projected_col_idx)
/// for each table that was loaded with projection + skip_expand.
async fn load_tables_smart(
    command: &serde_json::Value,
    store: &PartitionStore,
    data_dir: Option<&Path>,
    hints: &HashMap<String, ScanHint>,
) -> anyhow::Result<(HashMap<String, Vec<RecordBatch>>, HashMap<String, HashMap<usize, usize>>)> {
    let tables_map = command["tables"]
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("missing 'tables' field"))?;

    let mut tables: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    let mut col_remaps: HashMap<String, HashMap<usize, usize>> = HashMap::new();

    for (name, spec_val) in tables_map {
        let spec = parse_table_spec(name, spec_val)?;

        // Pick the URI list to scan. Priority:
        //   1. explicit `uris` from the coordinator (delegation-driven)
        //   2. `{data_dir}/{store_key}/` (legacy bench Parquet path)
        let scan_uris: Vec<String> = if !spec.uris.is_empty() {
            spec.uris.clone()
        } else if let (Some(dir), Some(key)) = (data_dir, spec.store_key.as_deref()) {
            let parquet_dir = dir.join(key);
            if parquet_dir.exists() {
                vec![parquet_dir.to_string_lossy().into_owned()]
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        if !scan_uris.is_empty() {
            let hint = hints.get(name.as_str());
            let predicate = hint.and_then(|h| h.predicate.clone());

            // Column pruning: only read columns the query actually needs
            let projection = hint.and_then(|h| h.projection.clone());

            // Use skip_expand for all queries — multi-table JOINs are handled
            // via per-subtree column remapping in remap_plan_multi_table
            let has_projection = projection.is_some();

            let start = Instant::now();
            let batches = parquet_scan(&ParquetScanConfig {
                uris: scan_uris,
                predicate,
                projection: projection.clone(),
                batch_size: 1_048_576,
                skip_expand: has_projection,
                target_schema: None,
            })
            .await
            .map_err(|e| anyhow::anyhow!("Parquet scan error for '{}': {}", name, e))?;

            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            tracing::info!(
                "Loaded '{}' from Parquet: {} rows in {:.1}ms",
                name,
                rows,
                start.elapsed().as_secs_f64() * 1000.0,
            );

            // Build column remap: original_col_idx -> projected_batch_col_idx
            if let Some(ref proj_cols) = projection {
                let mut remap: HashMap<usize, usize> = HashMap::new();
                for (batch_idx, &orig_idx) in proj_cols.iter().enumerate() {
                    remap.insert(orig_idx, batch_idx);
                }
                col_remaps.insert(name.clone(), remap);
            }

            tables.insert(name.clone(), batches);
            continue;
        }

        // Fall back to in-memory PartitionStore — requires a store_key.
        let key = spec.store_key.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "table '{}' has empty 'uris' and no 'store_key' — cannot resolve",
                name
            )
        })?;
        let batches = store.get(key).await;
        if batches.is_empty() {
            return Err(anyhow::anyhow!(
                "table '{}' (store_key '{}') not found in store or Parquet",
                name,
                key
            ));
        }
        tables.insert(name.clone(), batches);
    }
    Ok((tables, col_remaps))
}

/// Remap column indices in the plan tree for multiple tables loaded with skip_expand.
/// Handles JOINs correctly by applying per-subtree remaps and adjusting offsets
/// at Join boundaries (left subtree uses left table's remap, right subtree uses right's,
/// above-join references are rewritten using a combined remap based on the new output schema).
fn remap_plan_multi_table(
    node: &mut prism_substrait::plan::PlanNode,
    table_remaps: &HashMap<String, HashMap<usize, usize>>,
) {
    let _ = remap_plan_rec(node, table_remaps);
}

/// Recursive helper. Returns an "outer remap" (original_col → new_col) describing
/// how to translate column references in THIS node's output schema.
fn remap_plan_rec(
    node: &mut prism_substrait::plan::PlanNode,
    table_remaps: &HashMap<String, HashMap<usize, usize>>,
) -> HashMap<usize, usize> {
    use prism_substrait::plan::PlanNode;

    match node {
        PlanNode::Scan { table_name, projection, .. } => {
            if let Some(remap) = table_remaps.get(table_name) {
                if let Some(proj) = projection {
                    for idx in proj.iter_mut() {
                        if let Some(&new_idx) = remap.get(idx) {
                            *idx = new_idx;
                        }
                    }
                }
                remap.clone()
            } else {
                HashMap::new()
            }
        }
        PlanNode::Filter { input, predicate } => {
            let input_remap = remap_plan_rec(input, table_remaps);
            remap_predicate_local(predicate, &input_remap);
            input_remap
        }
        PlanNode::Project { input, columns, expressions } => {
            let input_remap = remap_plan_rec(input, table_remaps);

            // Remap expression column refs using input_remap.
            for expr in expressions.iter_mut() {
                remap_expr_local(expr, &input_remap);
            }

            let n_cols_old = columns.len();
            let n_exprs = expressions.len();
            let mut output_remap: HashMap<usize, usize> = HashMap::new();

            if input_remap.is_empty() {
                // No pruning below — identity remap, columns unchanged.
                let n_total = n_cols_old + n_exprs;
                for i in 0..n_total {
                    output_remap.insert(i, i);
                }
                return output_remap;
            }

            // Input was pruned. Drop columns referring to unloaded input cols.
            // push_columns_down guarantees that any column referenced by the parent
            // is in input_remap — dropped cols are only those never referenced above.
            let mut new_columns = Vec::with_capacity(n_cols_old);
            for (old_pos, &c) in columns.iter().enumerate() {
                if let Some(&new_c) = input_remap.get(&c) {
                    let new_pos = new_columns.len();
                    new_columns.push(new_c);
                    output_remap.insert(old_pos, new_pos);
                }
            }
            let new_n_cols = new_columns.len();
            *columns = new_columns;

            // Expressions stay, but their output positions shift after dropped cols.
            for expr_idx in 0..n_exprs {
                let old_pos = n_cols_old + expr_idx;
                let new_pos = new_n_cols + expr_idx;
                output_remap.insert(old_pos, new_pos);
            }

            output_remap
        }
        PlanNode::Aggregate { input, group_by, aggregates } => {
            let input_remap = remap_plan_rec(input, table_remaps);
            for idx in group_by.iter_mut() {
                if let Some(&new_idx) = input_remap.get(idx) {
                    *idx = new_idx;
                }
            }
            for agg in aggregates.iter_mut() {
                if let Some(&new_idx) = input_remap.get(&agg.column) {
                    agg.column = new_idx;
                }
            }
            let n = group_by.len() + aggregates.len();
            (0..n).map(|i| (i, i)).collect()
        }
        PlanNode::Join { left, right, left_keys, right_keys, .. } => {
            let left_full_cols = find_scan_full_cols(left);
            let left_remap = remap_plan_rec(left, table_remaps);
            let right_remap = remap_plan_rec(right, table_remaps);

            // left_keys/right_keys index into each subtree's output. If the subtree
            // is a Scan (no inner Project), left_keys use left_remap. If there's an
            // inner Project, left_keys are already in projected local space [0..N-1]
            // (identity remap from Project returns {0→0, 1→1, ...}).
            for idx in left_keys.iter_mut() {
                if let Some(&new_idx) = left_remap.get(idx) {
                    *idx = new_idx;
                }
            }
            for idx in right_keys.iter_mut() {
                if let Some(&new_idx) = right_remap.get(idx) {
                    *idx = new_idx;
                }
            }

            // Build combined outer remap covering both original concat space
            // (when no inner Project) and projected concat space (when inner Project).
            // Original refs get offset math; projected refs (0..N) pass through unchanged.
            let left_proj_count = left_remap.values().map(|&v| v + 1).max().unwrap_or(0);
            let mut combined: HashMap<usize, usize> = HashMap::new();
            for (&orig, &new_idx) in &left_remap {
                combined.insert(orig, new_idx);
            }
            for (&orig, &new_idx) in &right_remap {
                combined.insert(left_full_cols + orig, left_proj_count + new_idx);
            }
            combined
        }
        PlanNode::Sort { input, sort_keys, .. } => {
            let input_remap = remap_plan_rec(input, table_remaps);
            for key in sort_keys.iter_mut() {
                if let Some(&new_idx) = input_remap.get(&key.column) {
                    key.column = new_idx;
                }
            }
            input_remap
        }
        PlanNode::Exchange { input, partition_keys, .. } => {
            let input_remap = remap_plan_rec(input, table_remaps);
            for idx in partition_keys.iter_mut() {
                if let Some(&new_idx) = input_remap.get(idx) {
                    *idx = new_idx;
                }
            }
            input_remap
        }
    }
}

fn find_scan_full_cols(node: &prism_substrait::plan::PlanNode) -> usize {
    use prism_substrait::plan::PlanNode;
    match node {
        PlanNode::Scan { schema, .. } => schema.fields().len(),
        PlanNode::Filter { input, .. }
        | PlanNode::Project { input, .. }
        | PlanNode::Aggregate { input, .. }
        | PlanNode::Sort { input, .. }
        | PlanNode::Exchange { input, .. } => find_scan_full_cols(input),
        PlanNode::Join { left, right, .. } => {
            find_scan_full_cols(left) + find_scan_full_cols(right)
        }
    }
}

fn remap_predicate_local(
    pred: &mut prism_executor::filter_project::Predicate,
    col_map: &HashMap<usize, usize>,
) {
    use prism_executor::filter_project::Predicate::*;
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
            remap_predicate_local(l, col_map);
            remap_predicate_local(r, col_map);
        }
        Not(inner) => {
            remap_predicate_local(inner, col_map);
        }
    }
}

fn remap_expr_local(
    expr: &mut prism_executor::filter_project::ScalarExpr,
    col_map: &HashMap<usize, usize>,
) {
    use prism_executor::filter_project::ScalarExpr::*;
    match expr {
        ColumnRef(c) => {
            if let Some(&new_idx) = col_map.get(c) { *c = new_idx; }
        }
        Literal(_) => {}
        BinaryOp { left, right, .. } => {
            remap_expr_local(left, col_map);
            remap_expr_local(right, col_map);
        }
        Negate(inner) => {
            remap_expr_local(inner, col_map);
        }
    }
}

/// Load tables from the partition store, concatenating all chunks into single RecordBatches.
/// Used by the legacy hardcoded query path which expects single RecordBatch per table.
///
/// This path is in-memory only (DoPut-pushed data); it accepts the v2 per-table
/// spec but requires `store_key` to be set.
async fn load_tables(
    command: &serde_json::Value,
    store: &PartitionStore,
) -> anyhow::Result<HashMap<String, RecordBatch>> {
    let tables_map = command["tables"]
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("missing 'tables' field"))?;

    let mut tables: HashMap<String, RecordBatch> = HashMap::new();
    for (name, spec_val) in tables_map {
        let spec = parse_table_spec(name, spec_val)?;
        let key = spec.store_key.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "legacy load_tables requires 'store_key' on table '{}'",
                name
            )
        })?;
        let batches = store.get(key).await;
        if batches.is_empty() {
            return Err(anyhow::anyhow!(
                "table '{}' (store_key '{}') not found in store",
                name,
                key
            ));
        }
        let schema = batches[0].schema();
        let merged = arrow::compute::concat_batches(&schema, &batches)?;
        tables.insert(name.clone(), merged);
    }
    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use arrow_flight::flight_service_server::FlightServiceServer;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use serde_json::json;
    use std::collections::HashMap as StdHashMap;
    use std::sync::Arc;
    use tonic::transport::Server;

    use prism_flight::shuffle_writer::ShuffleFlightService;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    /// v2 protocol — `uris` is empty and `store_key` points to an in-memory
    /// table pushed via `DoPut`. This mirrors the DoPut-driven bench path.
    #[tokio::test]
    async fn test_protocol_v2_store_key_fallback() {
        let store = PartitionStore::new();
        store.put("memtable/x", sample_batch()).await;

        let command = json!({
            "tables": {
                "x": { "uris": [], "store_key": "memtable/x" }
            }
        });

        let hints: HashMap<String, ScanHint> = HashMap::new();
        let (tables, remaps) =
            load_tables_smart(&command, &store, None, &hints).await.unwrap();
        assert_eq!(tables.len(), 1);
        assert!(remaps.is_empty());
        let batches = tables.get("x").unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3);

        // Equivalently via the legacy hardcoded path.
        let legacy = load_tables(&command, &store).await.unwrap();
        assert_eq!(legacy.get("x").unwrap().num_rows(), 3);
    }

    #[tokio::test]
    async fn test_protocol_v2_uri_list() {
        let tmp = tempfile::tempdir().unwrap();
        let file_path = tmp.path().join("t.parquet");
        let batch = sample_batch();
        let mut buf: Vec<u8> = Vec::new();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        {
            let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        std::fs::write(&file_path, &buf).unwrap();

        let store = PartitionStore::new();
        let command = json!({
            "tables": {
                "x": {
                    "uris": [file_path.to_string_lossy()],
                    "store_key": null
                }
            }
        });

        let hints: HashMap<String, ScanHint> = HashMap::new();
        let (tables, _remaps) = load_tables_smart(&command, &store, None, &hints).await.unwrap();

        let batches = tables.get("x").unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3);
    }

    #[tokio::test]
    async fn test_protocol_v2_rejects_old_bare_string() {
        let store = PartitionStore::new();
        store.put("tpch/lineitem", sample_batch()).await;
        let command = json!({ "tables": { "lineitem": "tpch/lineitem" } });

        let hints: HashMap<String, ScanHint> = HashMap::new();
        let err = load_tables_smart(&command, &store, None, &hints)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("must be an object"),
            "expected v2 parse error, got: {}",
            err
        );
    }

    #[test]
    fn reducer_merge_reconstructs_original_aggregates() {
        let original_aggs = vec![
            AggExpr {
                column: 1,
                func: AggFunc::Sum,
                output_name: "agg_0".into(),
            },
            AggExpr {
                column: 1,
                func: AggFunc::Count,
                output_name: "agg_1".into(),
            },
            AggExpr {
                column: 1,
                func: AggFunc::Avg,
                output_name: "agg_2".into(),
            },
        ];
        let (worker_aggs, agg_mapping) = expand_worker_aggregates(&original_aggs);
        let merge_config = build_final_merge_config(1, &worker_aggs).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("status", DataType::Utf8, false),
            Field::new("agg_0", DataType::Float64, true),
            Field::new("agg_1", DataType::Int64, true),
            Field::new("agg_2", DataType::Float64, true),
            Field::new("agg_3", DataType::Int64, true),
        ]));

        let partial_a = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["F", "O"])),
                Arc::new(Float64Array::from(vec![100.0, 50.0])),
                Arc::new(Int64Array::from(vec![2, 1])),
                Arc::new(Float64Array::from(vec![100.0, 50.0])),
                Arc::new(Int64Array::from(vec![2, 1])),
            ],
        )
        .unwrap();
        let partial_b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["F", "P"])),
                Arc::new(Float64Array::from(vec![30.0, 70.0])),
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![30.0, 70.0])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        let merged = prism_executor::hash_aggregate::hash_aggregate_batches(
            &[partial_a, partial_b],
            &merge_config,
        )
        .unwrap();
        let final_batch = reconstruct_original_aggregate_batch(
            &merged,
            1,
            &original_aggs,
            &agg_mapping,
        )
        .unwrap();

        let statuses = final_batch.column(0).as_string::<i32>();
        let sum = final_batch.column(1).as_primitive::<arrow_array::types::Float64Type>();
        let count = final_batch.column(2).as_primitive::<arrow_array::types::Int64Type>();
        let avg = final_batch.column(3).as_primitive::<arrow_array::types::Float64Type>();

        let mut rows = StdHashMap::new();
        for row in 0..final_batch.num_rows() {
            rows.insert(
                statuses.value(row).to_string(),
                (sum.value(row), count.value(row), avg.value(row)),
            );
        }

        assert_eq!(rows["F"], (130.0, 3, 130.0 / 3.0));
        assert_eq!(rows["O"], (50.0, 1, 50.0));
        assert_eq!(rows["P"], (70.0, 2, 35.0));
    }

    #[tokio::test]
    async fn partial_batch_transport_round_trips_through_flight() {
        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = reserved.local_addr().unwrap();
        drop(reserved);

        let store = Arc::new(PartitionStore::new());
        let service = ShuffleFlightService::new(store.clone());
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve(addr)
                .await
                .unwrap();
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("status", DataType::Utf8, false),
                Field::new("agg_0", DataType::Float64, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["F"])),
                Arc::new(Float64Array::from(vec![42.0])),
            ],
        )
        .unwrap();

        send_partial_batch(&addr.to_string(), "__partialagg/test/0", batch)
            .await
            .unwrap();
        let received = wait_for_partial_batches(
            &store,
            &[String::from("__partialagg/test/0")],
            Duration::from_secs(2),
        )
        .await
        .unwrap();

        server.abort();

        assert_eq!(received.len(), 1);
        assert_eq!(received[0].num_rows(), 1);
        assert_eq!(received[0].column(0).as_string::<i32>().value(0), "F");
        assert_eq!(
            received[0]
                .column(1)
                .as_primitive::<arrow_array::types::Float64Type>()
                .value(0),
            42.0
        );
    }
}
