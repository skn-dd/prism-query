#!/bin/bash
# Prism SF100 Benchmark Runner
# Prerequisites: setup-ec2.sh completed, Trino running on port 8080
#
# Memory plan for r7g.4xlarge (128 GB):
#   Worker 0: ~50 GB (600M lineitem + 150M orders)
#   Worker 1: ~50 GB (same)
#   Trino JVM: ~12 GB
#   OS/buffers: ~16 GB
#   Total: ~128 GB

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
WORKER_BIN="$PROJECT_DIR/native/target/release/prism-worker"
SF=100

echo "=== Prism SF${SF} Benchmark ==="
echo "Project: $PROJECT_DIR"
echo ""

# Check binary exists
if [ ! -f "$WORKER_BIN" ]; then
    echo "ERROR: Worker binary not found at $WORKER_BIN"
    echo "Run: bash deploy/setup-ec2.sh first"
    exit 1
fi

# Kill existing workers
echo "Stopping existing workers..."
pkill -f prism-worker 2>/dev/null || true
sleep 2

# Start workers
echo "Starting Rust workers..."
"$WORKER_BIN" --port 50051 &
WORKER0_PID=$!
"$WORKER_BIN" --port 50052 &
WORKER1_PID=$!
sleep 3

echo "Worker 0 PID: $WORKER0_PID (port 50051)"
echo "Worker 1 PID: $WORKER1_PID (port 50052)"

# Load SF100 data (chunked, 5M rows per chunk)
echo ""
echo "Loading SF${SF} data into workers..."
python3 - <<'PYEOF'
import pyarrow.flight as flight
import json, time, sys

SF = 100.0
CHUNK_SIZE = 5_000_000  # 5M rows per chunk to avoid huge allocations

workers = [50051, 50052]

for port in workers:
    client = flight.connect(f"grpc://localhost:{port}")
    for table in ["lineitem", "orders"]:
        rows = int(6_000_000 * SF) if table == "lineitem" else int(1_500_000 * SF)
        print(f"Worker {port}: generating {table} SF{SF:.0f} ({rows:,} rows, {CHUNK_SIZE//1_000_000}M chunks)...", flush=True)
        t0 = time.time()
        cmd = json.dumps({
            "query": "datagen",
            "table": table,
            "sf": SF,
            "store_key": f"tpch/{table}",
            "result_key": f"status/datagen_{table}",
            "chunk_size": CHUNK_SIZE
        }).encode()
        action = flight.Action("execute", cmd)
        for result in client.do_action(action):
            msg = result.body.to_pybytes().decode()
            elapsed = time.time() - t0
            print(f"  Done in {elapsed:.1f}s: {msg}", flush=True)

print(f"\nSF{SF:.0f} data loaded on all workers.")
PYEOF

# Verify
echo ""
echo "Verifying row count..."
python3 -c "
import json, urllib.request, time
sql = 'SELECT COUNT(*) FROM lineitem'
req = urllib.request.Request('http://localhost:8080/v1/statement', data=sql.encode(),
    headers={'X-Trino-User':'bench','X-Trino-Catalog':'prism','X-Trino-Schema':'tpch'})
resp = json.loads(urllib.request.urlopen(req).read())
uri = resp.get('nextUri','')
while uri:
    time.sleep(0.1)
    req = urllib.request.Request(uri, headers={'X-Trino-User':'bench'})
    resp = json.loads(urllib.request.urlopen(req).read())
    uri = resp.get('nextUri','')
    if 'data' in resp: print(f'Lineitem rows: {resp[\"data\"][0][0]:,}')
"

# Run benchmark
echo ""
echo "Running SF${SF} benchmark..."
python3 "$PROJECT_DIR/trino-bench/benchmark-sf100.py"

echo ""
echo "=== Benchmark Complete ==="
echo "Results: $PROJECT_DIR/trino-bench/results-sf100.json"
