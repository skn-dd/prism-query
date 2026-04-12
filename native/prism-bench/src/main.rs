//! Prism Worker — Arrow Flight server that receives data and executes
//! query plans from the Trino coordinator.
//!
//! Usage:
//!   prism-worker --port 50051
//!
//! The worker exposes three Flight endpoints:
//!   - DoPut:    Receive Arrow RecordBatches (table data from coordinator)
//!   - DoAction: Execute query plans on local data
//!   - DoGet:    Serve query results back to coordinator

mod handler;
mod queries;
mod datagen;

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let addr = format!("0.0.0.0:{}", args.port).parse()?;

    let store = Arc::new(PartitionStore::new());
    let service = ShuffleFlightService::new(store.clone());

    // Register the query execution handler
    service.set_action_handler(Box::new(QueryHandler::new())).await;

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
