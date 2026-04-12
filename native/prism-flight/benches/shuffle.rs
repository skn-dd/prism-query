//! Shuffle throughput benchmark — measures partition + Arrow Flight
//! server/client round-trip performance.
//!
//! Tests:
//! 1. Hash partitioning throughput (CPU-bound)
//! 2. Arrow IPC serialization/deserialization throughput
//! 3. Full Flight server → client round-trip (localhost gRPC)
//! 4. Partition skew analysis (how evenly data distributes)

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::Ticket;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use tonic::transport::Server;

use prism_flight::shuffle_writer::{partition_batch, PartitionStore, ShuffleFlightService};

// ─── Data generators ─────────────────────────────────────────────────────────

fn make_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
    ]));

    let regions = ["east", "west", "north", "south", "central"];
    let categories = [
        "electronics", "clothing", "food", "automotive", "healthcare",
        "sports", "entertainment", "education", "finance", "travel",
    ];

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..n).map(|i| regions[i % regions.len()]).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..n).map(|i| (i as f64) * 1.5 + 10.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n).map(|i| categories[i % categories.len()]).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

// ─── Benchmark runner ────────────────────────────────────────────────────────

struct BenchResult {
    name: String,
    rows: usize,
    elapsed_ms: f64,
    throughput_mrows_sec: f64,
    extra: String,
}

impl BenchResult {
    fn print(&self) {
        eprintln!(
            "  {:50} {:>10} rows  {:>8.2}ms  {:>8.2}M rows/s  {}",
            self.name, self.rows, self.elapsed_ms, self.throughput_mrows_sec, self.extra
        );
    }
}

fn bench_op<F>(name: &str, rows: usize, iterations: usize, extra: &str, mut f: F) -> BenchResult
where
    F: FnMut() -> (),
{
    for _ in 0..2 { f(); }

    let start = Instant::now();
    for _ in 0..iterations { f(); }
    let total = start.elapsed();
    let per_iter = total.as_secs_f64() / iterations as f64;

    BenchResult {
        name: name.to_string(),
        rows,
        elapsed_ms: per_iter * 1000.0,
        throughput_mrows_sec: (rows as f64) / per_iter / 1_000_000.0,
        extra: extra.to_string(),
    }
}

// ─── Partition throughput ────────────────────────────────────────────────────

fn bench_partition(batch: &RecordBatch, label: &str) -> Vec<BenchResult> {
    let n = batch.num_rows();
    let iters = if n >= 1_000_000 { 5 } else { 20 };
    let mut results = Vec::new();

    for &num_parts in &[4, 8, 16, 32] {
        let r = bench_op(
            &format!("partition_batch into {} parts [{}]", num_parts, label),
            n,
            iters,
            &format!("→ {} partitions", num_parts),
            || { partition_batch(batch, &[0, 1], num_parts).unwrap(); },
        );
        results.push(r);
    }

    // Skew analysis
    let partitions = partition_batch(batch, &[0, 1], 16).unwrap();
    let sizes: Vec<usize> = partitions.iter().map(|b| b.num_rows()).collect();
    let min = sizes.iter().min().unwrap();
    let max = sizes.iter().max().unwrap();
    let avg = n / 16;
    let skew = *max as f64 / avg as f64;
    eprintln!(
        "    Skew analysis (16 parts): min={}, max={}, avg={}, skew_ratio={:.2}x",
        min, max, avg, skew
    );

    results
}

// ─── Arrow IPC serialization round-trip ──────────────────────────────────────

fn bench_ipc_roundtrip(batch: &RecordBatch, label: &str) -> Vec<BenchResult> {
    let n = batch.num_rows();
    let iters = if n >= 1_000_000 { 5 } else { 20 };
    let mut results = Vec::new();

    // Serialize
    let r = bench_op(
        &format!("Arrow IPC serialize [{}]", label),
        n,
        iters,
        "",
        || {
            let mut buf = Vec::with_capacity(n * 64);
            let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref()).unwrap();
            writer.write(batch).unwrap();
            writer.finish().unwrap();
        },
    );
    results.push(r);

    // Serialize to get bytes for deserialize bench
    let mut buf = Vec::with_capacity(n * 64);
    let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref()).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    let ipc_bytes = buf;

    let size_mb = ipc_bytes.len() as f64 / 1_048_576.0;
    eprintln!("    IPC payload: {:.2}MB for {} rows ({:.0} bytes/row)", size_mb, n, ipc_bytes.len() as f64 / n as f64);

    // Deserialize
    let r = bench_op(
        &format!("Arrow IPC deserialize [{}]", label),
        n,
        iters,
        &format!("{:.2}MB", size_mb),
        || {
            let reader = StreamReader::try_new(&ipc_bytes[..], None).unwrap();
            let _batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
        },
    );
    results.push(r);

    // Full round-trip
    let r = bench_op(
        &format!("Arrow IPC full round-trip [{}]", label),
        n,
        iters,
        "",
        || {
            let mut buf = Vec::with_capacity(n * 64);
            {
                let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref()).unwrap();
                writer.write(batch).unwrap();
                writer.finish().unwrap();
            }
            let reader = StreamReader::try_new(&buf[..], None).unwrap();
            let _batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
        },
    );
    results.push(r);

    results
}

// ─── Flight gRPC round-trip (localhost) ──────────────────────────────────────

fn bench_flight_roundtrip(batch: &RecordBatch, label: &str) -> Vec<BenchResult> {
    let n = batch.num_rows();
    let iters = if n >= 1_000_000 { 3 } else { 10 };
    let mut results = Vec::new();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let store = Arc::new(PartitionStore::new());
        let service = ShuffleFlightService::new(store.clone());

        // Start server
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        // Give server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Store data
        let key = "bench/partition/0";
        store.put(key, batch.clone()).await;

        // Benchmark fetch
        let uri = format!("http://127.0.0.1:{}", port);
        let channel = tonic::transport::Channel::from_shared(uri)
            .unwrap()
            .connect()
            .await
            .unwrap();

        // Warmup
        for _ in 0..2 {
            let mut client = arrow_flight::flight_service_client::FlightServiceClient::new(channel.clone());
            let ticket = Ticket::new(key.as_bytes().to_vec());
            let response = client.do_get(ticket).await.unwrap();
            let stream = FlightRecordBatchStream::new_from_flight_data(
                response.into_inner().map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
            );
            let _batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        }

        // Timed
        let start = Instant::now();
        for _ in 0..iters {
            let mut client = arrow_flight::flight_service_client::FlightServiceClient::new(channel.clone());
            let ticket = Ticket::new(key.as_bytes().to_vec());
            let response = client.do_get(ticket).await.unwrap();
            let stream = FlightRecordBatchStream::new_from_flight_data(
                response.into_inner().map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
            );
            let _batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        }
        let total = start.elapsed();
        let per_iter = total.as_secs_f64() / iters as f64;

        results.push(BenchResult {
            name: format!("Flight gRPC do_get (localhost) [{}]", label),
            rows: n,
            elapsed_ms: per_iter * 1000.0,
            throughput_mrows_sec: (n as f64) / per_iter / 1_000_000.0,
            extra: "full round-trip".to_string(),
        });
    });

    results
}

// ─── Main ────────────────────────────────────────────────────────────────────

fn main() {
    eprintln!("╔══════════════════════════════════════════════════════════════════════════════╗");
    eprintln!("║         Prism Shuffle Throughput Benchmark (release build)                    ║");
    eprintln!("╚══════════════════════════════════════════════════════════════════════════════╝");
    eprintln!();

    for &(size, label) in &[
        (100_000, "100K"),
        (1_000_000, "1M"),
        (5_000_000, "5M"),
    ] {
        eprintln!("━━━ {} rows ━━━", label);
        let batch = make_batch(size);
        eprintln!();

        eprintln!("  Hash Partitioning:");
        for r in bench_partition(&batch, label) {
            r.print();
        }
        eprintln!();

        eprintln!("  Arrow IPC Serialization:");
        for r in bench_ipc_roundtrip(&batch, label) {
            r.print();
        }
        eprintln!();

        eprintln!("  Arrow Flight gRPC:");
        for r in bench_flight_roundtrip(&batch, label) {
            r.print();
        }
        eprintln!();
    }

    eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    eprintln!("Trino Exchange Baseline (REST-based, single-thread estimates):");
    eprintln!("  100K rows serialize+transfer:    ~50-100ms  (custom wire format)");
    eprintln!("  1M rows serialize+transfer:      ~400-800ms (HTTP overhead + deser)");
    eprintln!("  5M rows serialize+transfer:      ~2-4s      (memory copies + GC pressure)");
    eprintln!();
    eprintln!("Arrow Flight advantages:");
    eprintln!("  • Zero-copy IPC (no deserialization on receive)");
    eprintln!("  • gRPC streaming (no HTTP request/response overhead per page)");
    eprintln!("  • Columnar layout preserved end-to-end");
}
