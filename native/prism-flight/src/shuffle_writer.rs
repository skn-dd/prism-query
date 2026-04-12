//! Shuffle writer — partitions output RecordBatches by hash and serves
//! them via Arrow Flight for remote workers to pull.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, UInt32Array};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type, Float64Type};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use arrow_schema::DataType;
use futures::stream::{self, BoxStream, StreamExt};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};

use crate::PartitionId;

/// In-memory partition store.
#[derive(Default)]
pub struct PartitionStore {
    partitions: RwLock<HashMap<String, Vec<RecordBatch>>>,
}

impl PartitionStore {
    pub fn new() -> Self { Self::default() }

    pub async fn put(&self, key: &str, batch: RecordBatch) {
        let mut map = self.partitions.write().await;
        map.entry(key.to_string()).or_default().push(batch);
    }

    pub async fn get(&self, key: &str) -> Vec<RecordBatch> {
        let map = self.partitions.read().await;
        map.get(key).cloned().unwrap_or_default()
    }

    pub async fn clear(&self, key: &str) {
        let mut map = self.partitions.write().await;
        map.remove(key);
    }
}

/// Partition a RecordBatch by hash of specified key columns into N output partitions.
pub fn partition_batch(
    batch: &RecordBatch,
    partition_keys: &[usize],
    num_partitions: usize,
) -> anyhow::Result<Vec<RecordBatch>> {
    if num_partitions == 0 { return Ok(vec![]); }
    if num_partitions == 1 { return Ok(vec![batch.clone()]); }

    let num_rows = batch.num_rows();

    let mut partition_indices: Vec<Vec<u32>> = vec![Vec::new(); num_partitions];
    for row in 0..num_rows {
        let h = hash_row(batch, partition_keys, row);
        let partition = (h as usize) % num_partitions;
        partition_indices[partition].push(row as u32);
    }

    let mut output = Vec::with_capacity(num_partitions);
    for indices in &partition_indices {
        if indices.is_empty() {
            output.push(RecordBatch::new_empty(batch.schema()));
        } else {
            let idx_array = UInt32Array::from(indices.clone());
            let columns: Vec<_> = batch.columns().iter()
                .map(|col| arrow::compute::take(col, &idx_array, None))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            output.push(RecordBatch::try_new(batch.schema(), columns)?);
        }
    }

    Ok(output)
}

fn hash_row(batch: &RecordBatch, key_cols: &[usize], row: usize) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for &col_idx in key_cols {
        let col = batch.column(col_idx);
        hash_value(col.as_ref(), row, &mut hasher);
    }
    hasher.finish()
}

fn hash_value(array: &dyn Array, row: usize, hasher: &mut impl Hasher) {
    if array.is_null(row) { 0u8.hash(hasher); return; }
    match array.data_type() {
        DataType::Int32 => array.as_primitive::<Int32Type>().value(row).hash(hasher),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row).hash(hasher),
        DataType::Float64 => array.as_primitive::<Float64Type>().value(row).to_bits().hash(hasher),
        DataType::Utf8 => array.as_string::<i32>().value(row).hash(hasher),
        _ => format!("{:?}", array.slice(row, 1)).hash(hasher),
    }
}

/// Flight service serving partition data.
pub struct ShuffleFlightService {
    store: Arc<PartitionStore>,
}

impl ShuffleFlightService {
    pub fn new(store: Arc<PartitionStore>) -> Self { Self { store } }
    pub fn into_server(self) -> FlightServiceServer<Self> { FlightServiceServer::new(self) }
}

#[tonic::async_trait]
impl FlightService for ShuffleFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(&self, _req: Request<Streaming<HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(&self, _req: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }
    async fn get_flight_info(&self, _req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }
    async fn get_schema(&self, _req: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let partition_key = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let batches = self.store.get(&partition_key).await;
        if batches.is_empty() {
            return Err(Status::not_found(format!("partition {} not found", partition_key)));
        }

        let schema = batches[0].schema();
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::iter(batches.into_iter().map(Ok)))
            .map(|result| result.map_err(|e| Status::internal(e.to_string())));

        Ok(Response::new(Box::pin(flight_stream)))
    }

    async fn do_put(&self, _req: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }
    async fn do_action(&self, _req: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }
    async fn list_actions(&self, _req: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
    async fn do_exchange(&self, _req: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
    async fn poll_flight_info(&self, _req: Request<FlightDescriptor>) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
            Arc::new(StringArray::from(vec!["east", "west", "east", "north", "west", "east", "north", "west"])),
        ]).unwrap()
    }

    #[test]
    fn test_partition_batch() {
        let batch = test_batch();
        let partitions = partition_batch(&batch, &[1], 4).unwrap();
        assert_eq!(partitions.len(), 4);
        let total_rows: usize = partitions.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 8);
    }

    #[tokio::test]
    async fn test_partition_store() {
        let store = PartitionStore::new();
        let batch = test_batch();
        store.put("s1/p0", batch.clone()).await;
        store.put("s1/p0", batch.clone()).await;
        assert_eq!(store.get("s1/p0").await.len(), 2);
        store.clear("s1/p0").await;
        assert!(store.get("s1/p0").await.is_empty());
    }
}
