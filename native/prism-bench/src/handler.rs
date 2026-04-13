//! Query execution handler — implements the ActionHandler trait
//! to execute TPC-H queries on local data stored in PartitionStore.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use base64::Engine;

use prism_flight::shuffle_writer::{ActionHandler, PartitionStore};

use crate::{datagen, queries};

/// Handles "execute" DoAction commands from the coordinator.
pub struct QueryHandler;

impl QueryHandler {
    pub fn new() -> Self { Self }
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

            // Load tables from the store
            let tables = load_tables(&command, store).await?;

            let start = Instant::now();
            let plan = prism_substrait::consumer::consume_plan(&plan_bytes)
                .map_err(|e| anyhow::anyhow!("Substrait consume error: {}", e))?;
            let result = prism_substrait::executor::execute_plan(&plan.root, &tables)
                .map_err(|e| anyhow::anyhow!("Substrait execute error: {}", e))?;
            let elapsed = start.elapsed();

            tracing::info!(
                "Executed Substrait plan on {} tables → {} rows in {:.2}ms",
                tables.len(),
                result.num_rows(),
                elapsed.as_secs_f64() * 1000.0,
            );
            return Ok(result);
        }

        let query_name = command["query"].as_str()
            .ok_or_else(|| anyhow::anyhow!("missing 'query' field"))?;

        // --- Data generation action ---
        if query_name == "datagen" {
            let table = command["table"].as_str()
                .ok_or_else(|| anyhow::anyhow!("datagen: missing 'table' field"))?;
            let sf = command["sf"].as_f64().unwrap_or(1.0);
            let store_key = command["store_key"].as_str()
                .ok_or_else(|| anyhow::anyhow!("datagen: missing 'store_key' field"))?;

            let start = Instant::now();
            let batch = match table {
                "lineitem" => datagen::make_lineitem(sf),
                "orders" => datagen::make_orders(sf),
                other => return Err(anyhow::anyhow!("datagen: unknown table '{}'", other)),
            };
            let rows = batch.num_rows();
            store.put(store_key, batch).await;
            let elapsed = start.elapsed();

            tracing::info!(
                "Generated {} ({} rows, SF={}) stored as '{}' in {:.0}ms",
                table, rows, sf, store_key, elapsed.as_secs_f64() * 1000.0,
            );

            // Return a small status batch
            let schema = Arc::new(Schema::new(vec![
                Field::new("table", DataType::Utf8, false),
                Field::new("rows", DataType::Int64, false),
                Field::new("store_key", DataType::Utf8, false),
            ]));
            return Ok(RecordBatch::try_new(schema, vec![
                Arc::new(StringArray::from(vec![table])),
                Arc::new(Int64Array::from(vec![rows as i64])),
                Arc::new(StringArray::from(vec![store_key])),
            ])?);
        }

        // --- Hardcoded query dispatch (legacy benchmark path) ---
        let tables = load_tables(&command, store).await?;

        let start = Instant::now();
        let result = match query_name {
            "q1" => queries::execute_q1(&tables)?,
            "q3" => queries::execute_q3(&tables)?,
            "q6" => queries::execute_q6(&tables)?,
            "scan" => {
                tables.get("lineitem")
                    .ok_or_else(|| anyhow::anyhow!("lineitem table not found"))?
                    .clone()
            }
            other => return Err(anyhow::anyhow!("unknown query: {}", other)),
        };
        let elapsed = start.elapsed();

        tracing::info!(
            "Executed {} on {} tables → {} rows in {:.2}ms",
            query_name,
            tables.len(),
            result.num_rows(),
            elapsed.as_secs_f64() * 1000.0,
        );

        Ok(result)
    }
}

/// Load tables from the partition store based on the "tables" map in the command JSON.
async fn load_tables(
    command: &serde_json::Value,
    store: &PartitionStore,
) -> anyhow::Result<HashMap<String, RecordBatch>> {
    let tables_map = command["tables"].as_object()
        .ok_or_else(|| anyhow::anyhow!("missing 'tables' field"))?;

    let mut tables: HashMap<String, RecordBatch> = HashMap::new();
    for (name, key_val) in tables_map {
        let key = key_val.as_str()
            .ok_or_else(|| anyhow::anyhow!("table key must be a string"))?;
        let batches = store.get(key).await;
        if batches.is_empty() {
            return Err(anyhow::anyhow!("table '{}' (key '{}') not found in store", name, key));
        }
        let schema = batches[0].schema();
        let merged = arrow::compute::concat_batches(&schema, &batches)?;
        tables.insert(name.clone(), merged);
    }
    Ok(tables)
}
