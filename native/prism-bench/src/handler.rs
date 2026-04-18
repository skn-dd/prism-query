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

            tracing::info!("DBG_PLAN: {:#?}", plan.root);
            for (t, h) in &hints {
                tracing::info!("DBG_HINT: table={} projection={:?} predicate={:?}", t, h.projection, h.predicate);
            }

            // Fast path: unfiltered COUNT(*) from Parquet metadata only (no data reading).
            // Only used when there is NO predicate — filtered COUNT(*) must go through
            // the executor for exact results (row-group skipping only gives an overestimate).
            if let Some(data_dir) = self.data_dir.as_deref() {
                if let Some((table_name, pred)) = detect_count_star(&plan.root) {
                    if pred.is_none() {
                        if let Some(store_key) = command["tables"]
                            .as_object()
                            .and_then(|m| m.get(&table_name))
                            .and_then(|v| v.as_str())
                        {
                            let parquet_dir = data_dir.join(store_key);
                            if parquet_dir.exists() {
                                let count = parquet_row_count(
                                    &parquet_dir,
                                    None,
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
            }

            // Load tables — Parquet with pushdown if available, else in-memory
            let (tables, col_remaps) = load_tables_smart(&command, store, self.data_dir.as_deref(), &hints).await?;
            for (t, batches) in &tables {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                let cols = batches.first().map(|b| b.num_columns()).unwrap_or(0);
                let schema_str = batches.first().map(|b| format!("{:?}", b.schema().fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>())).unwrap_or_default();
                tracing::info!("DBG_LOAD: table={} rows={} cols={} nbatches={} schema={}", t, rows, cols, batches.len(), schema_str);
            }

            // Remap column indices for projected Parquet loading with skip_expand.
            // Handles single-table (trivial) and multi-table JOIN cases (per-subtree remap).
            let mut plan_root = plan.root.clone();
            if !col_remaps.is_empty() {
                remap_plan_multi_table(&mut plan_root, &col_remaps);
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

                // Use skip_expand for all queries — multi-table JOINs are handled
                // via per-subtree column remapping in remap_plan_multi_table
                let has_projection = projection.is_some();

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
