//! Prism Worker — Arrow Flight server that receives data and executes
//! query plans from the Trino coordinator.
//!
//! Usage:
//!   prism-worker --port 50051
//!   prism-worker --port 50051 --data-dir /data/prism
//!   prism-worker --config /etc/prism/worker.toml
//!
//! The worker exposes three Flight endpoints:
//!   - DoPut:    Receive Arrow RecordBatches (table data from coordinator)
//!   - DoAction: Execute query plans on local data
//!   - DoGet:    Serve query results back to coordinator

mod config;
mod handler;
mod queries;
mod datagen;

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use tonic::transport::Server;
use tracing::Level;

use prism_flight::shuffle_writer::{PartitionStore, ShuffleFlightService};
use handler::QueryHandler;

#[derive(Parser)]
#[command(name = "prism-worker", about = "Prism Arrow Flight worker")]
struct Args {
    /// Path to the worker TOML config. If omitted, the worker probes
    /// /etc/prism/worker.toml and falls back to built-in defaults.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Port to listen on. Overrides the port portion of `server.bind`
    /// from the config file.
    #[arg(long)]
    port: Option<u16>,

    /// Directory for Parquet data files.
    /// When set, the worker checks for Parquet files at {data-dir}/{store_key}/
    /// before falling back to the in-memory PartitionStore.
    #[arg(long)]
    data_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let cli = config::CliArgs {
        config: args.config.clone(),
        port: args.port,
        data_dir: args.data_dir.clone(),
    };
    let cfg = config::load(&cli)?;

    // Initialize tracing using the resolved log level. We fall back to INFO
    // if the configured level is unparseable rather than failing startup.
    let level = Level::from_str(&cfg.logging.level).unwrap_or(Level::INFO);
    tracing_subscriber::fmt().with_max_level(level).init();

    let addr = cfg.server.bind.parse()?;

    let store = Arc::new(PartitionStore::new());
    let service = ShuffleFlightService::new(store.clone());

    // Register the query execution handler. The legacy --data-dir flag
    // (and the equivalent config entry) drives the on-disk Parquet path.
    let handler = match cfg.data.data_dir.clone() {
        Some(dir) => {
            tracing::info!("Parquet data directory: {:?}", dir);
            eprintln!("Parquet data directory: {:?}", dir);
            QueryHandler::with_data_dir(dir)
        }
        None => QueryHandler::new(),
    };
    service.set_action_handler(Box::new(handler)).await;

    tracing::info!("Prism worker listening on {}", addr);
    eprintln!("Prism worker listening on {}", addr);

    let max_msg = cfg.server.max_message_size_mb * 1024 * 1024;
    let flight_svc = FlightServiceServer::new(service)
        .max_decoding_message_size(max_msg)
        .max_encoding_message_size(max_msg);

    Server::builder()
        .add_service(flight_svc)
        .serve(addr)
        .await?;

    Ok(())
}
