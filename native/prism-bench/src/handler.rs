//! Query execution handler — implements the ActionHandler trait
//! to execute TPC-H queries on local data stored in PartitionStore.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use base64::Engine;

use prism_executor::parquet_scan::{ParquetScanConfig, parquet_scan, parquet_row_count};
use prism_flight::shuffle_writer::{ActionHandler, PartitionStore};
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

            // Fast path: COUNT(*) from Parquet metadata only (no data reading)
            if let Some(data_dir) = self.data_dir.as_deref() {
                if let Some((table_name, pred)) = detect_count_star(&plan.root) {
                    if let Some(store_key) = command["tables"]
                        .as_object()
                        .and_then(|m| m.get(&table_name))
                        .and_then(|v| v.as_str())
                    {
                        let parquet_dir = data_dir.join(store_key);
                        if parquet_dir.exists() {
                            let count = parquet_row_count(
                                &parquet_dir,
                                pred.as_ref(),
                            ).map_err(|e| anyhow::anyhow!("Parquet row count error: {}", e))?;

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

            // Remap column indices if we used skip_expand for projected Parquet loading.
            // Only safe for single-table queries (avoid misremapping JOIN columns).
            let mut plan_root = plan.root.clone();
            if col_remaps.len() == 1 {
                for (_table_name, remap) in &col_remaps {
                    remap_plan_for_table(&mut plan_root, _table_name, remap);
                }
            }

            let result = prism_substrait::executor::execute_plan_chunked(&plan_root, &tables)
                .map_err(|e| anyhow::anyhow!("Substrait execute error: {}", e))?;
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

/// Load tables with Parquet support. Checks for Parquet files first,
/// falls back to PartitionStore.
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

    // Only use skip_expand for single-table queries to avoid JOIN column conflicts
    let is_single_table = tables_map.len() == 1;

    for (name, key_val) in tables_map {
        let key = key_val
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("table key must be a string"))?;

        // Try Parquet path first
        if let Some(dir) = data_dir {
            let parquet_dir = dir.join(key);
            if parquet_dir.exists() {
                let hint = hints.get(name.as_str());
                let predicate = hint.and_then(|h| h.predicate.clone());

                // Column pruning: only read columns the query actually needs
                let projection = hint.and_then(|h| h.projection.clone());

                // Use skip_expand to avoid null array creation overhead (single-table only)
                let has_projection = projection.is_some() && is_single_table;

                let start = Instant::now();
                let batches = parquet_scan(&ParquetScanConfig {
                    path: parquet_dir,
                    predicate,
                    projection: projection.clone(),
                    batch_size: 1_048_576,
                    skip_expand: has_projection,
                })
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
        }

        // Fall back to in-memory PartitionStore
        let batches = store.get(key).await;
        if batches.is_empty() {
            return Err(anyhow::anyhow!(
                "table '{}' (key '{}') not found in store or Parquet",
                name,
                key
            ));
        }
        tables.insert(name.clone(), batches);
    }
    Ok((tables, col_remaps))
}

/// Remap column indices in the plan tree for a specific table's scan node.
/// This is used when we loaded projected Parquet data with skip_expand=true,
/// meaning the batches have only the projected columns (not the full schema).
fn remap_plan_for_table(
    node: &mut prism_substrait::plan::PlanNode,
    table_name: &str,
    col_map: &HashMap<usize, usize>,
) {
    // Use the PlanNode::remap_columns method which recursively remaps all column references
    // Only remap if this plan actually references the given table
    // For simplicity, remap the entire tree (safe since each table has distinct column spaces)
    node.remap_columns(col_map);
}

/// Load tables from the partition store, concatenating all chunks into single RecordBatches.
/// Used by the legacy hardcoded query path which expects single RecordBatch per table.
async fn load_tables(
    command: &serde_json::Value,
    store: &PartitionStore,
) -> anyhow::Result<HashMap<String, RecordBatch>> {
    let tables_map = command["tables"]
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("missing 'tables' field"))?;

    let mut tables: HashMap<String, RecordBatch> = HashMap::new();
    for (name, key_val) in tables_map {
        let key = key_val
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("table key must be a string"))?;
        let batches = store.get(key).await;
        if batches.is_empty() {
            return Err(anyhow::anyhow!(
                "table '{}' (key '{}') not found in store",
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
