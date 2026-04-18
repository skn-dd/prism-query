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
use prism_flight::tls::{load_server_tls_config, TlsConfig as FlightTlsConfig};
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

    // Build the server. Wire TLS in unless explicitly disabled. Fail
    // fast on TLS misconfiguration so that a broken cert mount doesn't
    // silently degrade to plaintext.
    let mut builder = Server::builder();
    if cfg.tls.enabled {
        let tls_cfg = FlightTlsConfig {
            cert_path: cfg.tls.cert_path.clone(),
            key_path: cfg.tls.key_path.clone(),
            client_ca_path: if cfg.tls.client_ca_path.as_os_str().is_empty() {
                None
            } else {
                Some(cfg.tls.client_ca_path.clone())
            },
            client_cn_pattern: cfg.tls.client_cn_pattern.clone(),
        };
        let material = load_server_tls_config(&tls_cfg)
            .map_err(|e| anyhow::anyhow!("TLS config invalid: {}", e))?;
        let mode = if material.mtls { "mTLS" } else { "TLS (server-auth only)" };
        tracing::info!("Flight {} enabled (cert={:?})", mode, cfg.tls.cert_path);
        eprintln!("Flight {} enabled (cert={:?})", mode, cfg.tls.cert_path);
        // `.tls_config(ServerTlsConfig)` takes a value, not an Arc — we
        // keep an `Arc<ServerTlsConfig>` in TlsMaterial so a future
        // hot-reload path can swap the config atomically. For now we
        // clone out of the Arc once, at startup.
        builder = builder
            .tls_config((*material.server_config).clone())
            .map_err(|e| anyhow::anyhow!("failed to install TLS config: {}", e))?;
    } else {
        tracing::warn!(
            "TLS is DISABLED — plaintext Flight. This is for local \
             development only; production deployments MUST set \
             `tls.enabled = true`."
        );
        eprintln!(
            "WARNING: TLS disabled — plaintext Flight (development mode only)"
        );
    }

    builder
        .add_service(flight_svc)
        .serve(addr)
        .await?;

    Ok(())
}
