//! Prism Flight — Arrow Flight-based shuffle for inter-worker data transfer.
//!
//! Replaces Trino's REST-based `ExchangeOperator` with zero-copy Arrow Flight
//! (gRPC) transfers. Borrows patterns from Ballista's ShuffleWriterExec/ShuffleReaderExec.

pub mod shuffle_reader;
pub mod shuffle_writer;
pub mod tls;

/// Identifies a shuffle partition.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct PartitionId {
    /// Query/stage ID.
    pub stage_id: String,
    /// Partition number within the stage.
    pub partition: u32,
}

impl PartitionId {
    pub fn flight_descriptor(&self) -> String {
        format!("{}/partition/{}", self.stage_id, self.partition)
    }
}

/// Endpoint information for a remote worker's Flight server.
#[derive(Debug, Clone)]
pub struct WorkerEndpoint {
    pub host: String,
    pub port: u16,
}

impl WorkerEndpoint {
    pub fn uri(&self) -> String {
        format!("grpc://{}:{}", self.host, self.port)
    }
}
