//! Query execution handler — implements the ActionHandler trait
//! to execute TPC-H queries on local data stored in PartitionStore.

use std::collections::HashMap;
use std::time::Instant;

use arrow_array::RecordBatch;

use prism_flight::shuffle_writer::{ActionHandler, PartitionStore};

use crate::queries;

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
        let query_name = command["query"].as_str()
            .ok_or_else(|| anyhow::anyhow!("missing 'query' field"))?;

        // Load tables from the store
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
            // Concatenate all batches for this table
            let schema = batches[0].schema();
            let merged = arrow::compute::concat_batches(&schema, &batches)?;
            tables.insert(name.clone(), merged);
        }

        let start = Instant::now();
        let result = match query_name {
            "q1" => queries::execute_q1(&tables)?,
            "q3" => queries::execute_q3(&tables)?,
            "q6" => queries::execute_q6(&tables)?,
            "scan" => {
                // Just return the lineitem table
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
