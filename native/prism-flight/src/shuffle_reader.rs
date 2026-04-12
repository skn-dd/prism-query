//! Shuffle reader — pulls partitioned Arrow data from remote workers via Arrow Flight.

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use futures::TryStreamExt;
use tonic::transport::Channel;

use crate::WorkerEndpoint;

/// A reader that connects to a remote worker's Flight server.
pub struct ShuffleReader {
    endpoint: WorkerEndpoint,
}

impl ShuffleReader {
    pub fn new(endpoint: WorkerEndpoint) -> Self { Self { endpoint } }

    /// Fetch all RecordBatches for a given partition.
    pub async fn fetch_partition(&self, partition_key: &str) -> anyhow::Result<Vec<RecordBatch>> {
        let channel = Channel::from_shared(self.endpoint.uri())?
            .connect()
            .await?;

        let mut client = FlightServiceClient::new(channel);
        let ticket = Ticket::new(partition_key.as_bytes().to_vec());
        let response = client.do_get(ticket).await?;
        let stream = FlightRecordBatchStream::new_from_flight_data(
            response.into_inner().map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        Ok(batches)
    }

    /// Fetch from multiple workers in parallel.
    pub async fn fetch_from_multiple(
        endpoints: &[(WorkerEndpoint, String)],
    ) -> anyhow::Result<Vec<RecordBatch>> {
        let handles: Vec<_> = endpoints.iter().map(|(ep, key)| {
            let reader = ShuffleReader::new(ep.clone());
            let key = key.clone();
            tokio::spawn(async move { reader.fetch_partition(&key).await })
        }).collect();

        let mut all = Vec::new();
        for handle in handles {
            all.extend(handle.await??);
        }
        Ok(all)
    }
}
