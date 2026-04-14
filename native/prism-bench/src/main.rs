//! Prism Worker — Arrow Flight server that receives data and executes
//! query plans from the Trino coordinator.
//!
//! Usage:
//!   prism-worker --port 50051
//!   prism-worker --port 50051 --data-dir /data/prism
//!
//! The worker exposes three Flight endpoints:
//!   - DoPut:    Receive Arrow RecordBatches (table data from coordinator)
//!   - DoAction: Execute query plans on local data
//!   - DoGet:    Serve query results back to coordinator

mod handler;
mod queries;
mod datagen;

use std::path::PathBuf;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use tonic::transport::Server;

use prism_flight::shuffle_writer::{PartitionStore, ShuffleFlightService};
use handler::QueryHandler;

#[derive(Parser)]
#[command(name = "prism-worker", about = "Prism Arrow Flight worker")]
struct Args {
    /// Port to listen on
    #[arg(long, default_value = "50051")]
    port: u16,

    /// Directory for Parquet data files.
    /// When set, the worker checks for Parquet files at {data-dir}/{store_key}/
    /// before falling back to the in-memory PartitionStore.
    #[arg(long)]
    data_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let addr = format!("0.0.0.0:{}", args.port).parse()?;

    let store = Arc::new(PartitionStore::new());
    let service = ShuffleFlightService::new(store.clone());

    // Register the query execution handler
    let handler = match args.data_dir {
        Some(ref dir) => {
            tracing::info!("Parquet data directory: {:?}", dir);
            eprintln!("Parquet data directory: {:?}", dir);
            QueryHandler::with_data_dir(dir.clone())
        }
        None => QueryHandler::new(),
    };
    service.set_action_handler(Box::new(handler)).await;

    tracing::info!("Prism worker listening on {}", addr);
    eprintln!("Prism worker listening on {}", addr);

    // 256MB max message size to handle large RecordBatches
    let flight_svc = FlightServiceServer::new(service)
        .max_decoding_message_size(256 * 1024 * 1024)
        .max_encoding_message_size(256 * 1024 * 1024);

    Server::builder()
        .add_service(flight_svc)
        .serve(addr)
        .await?;

    Ok(())
}
