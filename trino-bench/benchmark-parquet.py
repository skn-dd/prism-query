#!/usr/bin/env python3
"""Prism Benchmark — In-Memory vs Parquet Storage at SF1/SF10/SF100.

Runs the 9-query benchmark suite through Trino against Prism workers,
comparing in-memory Vec<RecordBatch> vs sorted Parquet with row group skipping.

Usage:
    python3 benchmark-parquet.py [--sf 1,10,100] [--workers 4] [--data-dir /data/prism]
"""

import json, os, re, sys, time, urllib.request, urllib.error, argparse

SERVER = "http://localhost:8080"
WARMUP = 1
RUNS = 4

# ── Trino REST helpers ──

def submit_query(catalog, schema, sql):
    """Submit query via REST, poll to completion, return server-side elapsed ms + row count."""
    data = sql.encode()
    req = urllib.request.Request(
        f"{SERVER}/v1/statement", data=data,
        headers={"X-Trino-User": "benchmark", "X-Trino-Catalog": catalog, "X-Trino-Schema": schema},
    )
    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read())

    query_id = body.get("id", "")
    next_uri = body.get("nextUri", "")
    row_count = 0

    while next_uri:
        time.sleep(0.05)
        req = urllib.request.Request(next_uri, headers={"X-Trino-User": "benchmark"})
        try:
            with urllib.request.urlopen(req) as resp:
                body = json.loads(resp.read())
            next_uri = body.get("nextUri", "")
            if "data" in body:
                row_count += len(body["data"])
        except urllib.error.HTTPError:
            break

    if "error" in body:
        raise RuntimeError(f"Query failed: {body['error'].get('message', 'unknown')}")

    time.sleep(0.3)
    req = urllib.request.Request(f"{SERVER}/v1/query/{query_id}", headers={"X-Trino-User": "benchmark"})
    with urllib.request.urlopen(req) as resp:
        stats = json.loads(resp.read())

    if stats.get("state") == "FAILED":
        raise RuntimeError(f"Query FAILED: {stats.get('failureInfo',{}).get('message','unknown')}")

    qs = stats.get("queryStats", {})
    elapsed_str = qs.get("elapsedTime", "0ms")
    m = re.match(r"([0-9.]+)(ms|s|m|h)", elapsed_str)
    if m:
        val, unit = float(m.group(1)), m.group(2)
        if unit == "s": val *= 1000
        elif unit == "m": val *= 60000
        elif unit == "h": val *= 3600000
        return val, row_count
    return 0.0, row_count


def run_benchmark(name, catalog, schema, sql):
    print(f"  {name}", end="", flush=True)
    for _ in range(WARMUP):
        try: submit_query(catalog, schema, sql)
        except Exception as e: print(f" [warmup err: {e}]", end="", flush=True)

    times = []
    rows = 0
    for i in range(RUNS):
        try:
            ms, r = submit_query(catalog, schema, sql)
            times.append(ms)
            rows = r
            print(f" {ms:.0f}", end="", flush=True)
        except Exception as e:
            print(f" ERR", end="", flush=True)

    if not times:
        print(" FAILED", flush=True)
        return 0, 0

    times.sort()
    median = times[len(times)//2]
    print(f" -> {median:.0f}ms", flush=True)
    return median, rows


# ── Flight datagen helpers ──

def generate_inmemory_data(workers, sf, chunk_size=5_000_000):
    """Generate in-memory data on all workers via datagen command (sharded)."""
    import pyarrow.flight as flight

    num_workers = len(workers)
    for idx, port in enumerate(workers):
        client = flight.connect(f"grpc://localhost:{port}")
        for table in ["lineitem", "orders"]:
            total_rows = int(6_000_000 * sf) if table == "lineitem" else int(1_500_000 * sf)
            shard_rows = total_rows // num_workers
            shard_offset = idx * shard_rows
            if idx == num_workers - 1:
                shard_rows = total_rows - shard_offset
            print(f"  Worker {port}: {table} ({shard_rows:,} rows, offset {shard_offset:,})...", end="", flush=True)
            t0 = time.time()
            cmd = json.dumps({
                "query": "datagen",
                "table": table,
                "sf": sf,
                "store_key": f"tpch/{table}",
                "result_key": f"status/datagen_{table}",
                "chunk_size": chunk_size,
                "row_count": shard_rows,
                "row_offset": shard_offset
            }).encode()
            for r in client.do_action(flight.Action("execute", cmd)):
                pass
            print(f" {time.time()-t0:.1f}s", flush=True)


def generate_parquet_data(workers, sf, data_dir, row_group_size=1_000_000):
    """Generate sorted Parquet data on all workers via datagen_parquet command.

    Each worker writes to {data_dir}/worker{idx}/{store_key}/ which matches
    the --data-dir given to that worker in start_workers().
    """
    import pyarrow.flight as flight

    num_workers = len(workers)
    for idx, port in enumerate(workers):
        client = flight.connect(f"grpc://localhost:{port}")
        for table in ["lineitem", "orders"]:
            total_rows = int(6_000_000 * sf) if table == "lineitem" else int(1_500_000 * sf)
            shard_rows = total_rows // num_workers
            shard_offset = idx * shard_rows
            # Last worker gets remaining rows
            if idx == num_workers - 1:
                shard_rows = total_rows - shard_offset

            store_key = f"tpch/{table}"
            print(f"  Worker {port}: {table} Parquet ({shard_rows:,} rows, offset {shard_offset:,}, rg={row_group_size:,})...", end="", flush=True)
            t0 = time.time()
            cmd = json.dumps({
                "query": "datagen_parquet",
                "table": table,
                "store_key": store_key,
                "row_count": shard_rows,
                "row_offset": shard_offset,
                "row_group_size": row_group_size,
                "result_key": f"status/datagen_pq_{table}"
            }).encode()
            for r in client.do_action(flight.Action("execute", cmd)):
                pass
            print(f" {time.time()-t0:.1f}s", flush=True)


def stop_workers():
    """Kill existing prism-worker processes."""
    import subprocess
    subprocess.run(["pkill", "-f", "prism-worker"], capture_output=True)
    time.sleep(2)


def start_workers(worker_ports, data_dir=None):
    """Start prism-worker processes. Each worker gets its own data subdir."""
    import subprocess
    procs = []
    bin_path = os.path.expanduser("~/prism/native/target/release/prism-worker")
    for idx, port in enumerate(worker_ports):
        cmd = [bin_path, "--port", str(port)]
        if data_dir:
            worker_data_dir = os.path.join(data_dir, f"worker{idx}")
            os.makedirs(worker_data_dir, exist_ok=True)
            cmd.extend(["--data-dir", worker_data_dir])
        else:
            worker_data_dir = None
        env = dict(os.environ, RUST_LOG="info")
        p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=open(f"/tmp/worker_{port}.log", "w"), env=env)
        procs.append(p)
        dd_msg = f" --data-dir {worker_data_dir}" if worker_data_dir else ""
        print(f"  Started worker on port {port} (PID {p.pid}){dd_msg}", flush=True)
    time.sleep(3)
    return procs


# ── Queries (Prism prefixed columns) ──

QUERIES = {
    "scan": "SELECT COUNT(*) FROM lineitem",
    "filter": "SELECT COUNT(*) FROM lineitem WHERE l_quantity > 25 AND l_discount < 0.05",
    "agg": "SELECT l_returnflag, SUM(l_extendedprice), AVG(l_quantity), COUNT(*) FROM lineitem GROUP BY l_returnflag",
    "q1": """
        SELECT l_returnflag, l_linestatus,
               SUM(l_quantity), SUM(l_extendedprice),
               SUM(l_extendedprice * (1 - l_discount)),
               SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
               AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount), COUNT(*)
        FROM lineitem GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    """,
    "q6": """
        SELECT SUM(l_extendedprice * l_discount) as revenue
        FROM lineitem WHERE l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24
    """,
    "join": """
        SELECT o_orderstatus, SUM(l_extendedprice * (1 - l_discount)), COUNT(*)
        FROM lineitem JOIN orders ON l_orderkey = o_orderkey
        GROUP BY o_orderstatus ORDER BY o_orderstatus
    """,
    "orders_agg": "SELECT o_orderstatus, COUNT(*), SUM(o_totalprice) FROM orders GROUP BY o_orderstatus",
    "multi_agg": """
        SELECT l_returnflag, l_linestatus,
               SUM(l_quantity), SUM(l_extendedprice), MIN(l_discount), MAX(l_discount),
               AVG(l_quantity), AVG(l_extendedprice), COUNT(*)
        FROM lineitem GROUP BY l_returnflag, l_linestatus
    """,
    "topn": "SELECT l_orderkey, l_extendedprice, l_quantity FROM lineitem ORDER BY l_extendedprice DESC LIMIT 10",
}

QUERY_ORDER = ["scan", "filter", "agg", "q1", "q6", "join", "orders_agg", "multi_agg", "topn"]


def run_suite(label, results_prefix, sf):
    """Run the full 9-query suite, storing results."""
    results = {}
    for qname in QUERY_ORDER:
        key = f"{results_prefix}_sf{sf}_{qname}"
        median, rows = run_benchmark(f"{label}/{qname}", "prism", "tpch", QUERIES[qname])
        results[key] = {"ms": median, "rows": rows}
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sf", default="1,10,100", help="Scale factors (comma-separated)")
    parser.add_argument("--workers", type=int, default=4, help="Number of workers")
    parser.add_argument("--data-dir", default="/data/prism", help="Parquet data directory")
    parser.add_argument("--skip-inmemory", action="store_true", help="Skip in-memory benchmark")
    parser.add_argument("--skip-parquet", action="store_true", help="Skip Parquet benchmark")
    args = parser.parse_args()

    scale_factors = [int(x) for x in args.sf.split(",")]
    worker_ports = [50051 + i for i in range(args.workers)]
    data_dir = args.data_dir

    all_results = {}
    results_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results-parquet.json")

    # Load existing results if any
    if os.path.exists(results_file):
        with open(results_file) as f:
            all_results = json.load(f)

    print("=" * 70)
    print("  Prism Benchmark: In-Memory vs Parquet Storage")
    print(f"  Scale factors: {scale_factors}")
    print(f"  Workers: {args.workers} (ports {worker_ports[0]}-{worker_ports[-1]})")
    print(f"  Data dir: {data_dir}")
    print("=" * 70)

    for sf in scale_factors:
        li_rows = int(6_000_000 * sf)
        ord_rows = int(1_500_000 * sf)
        chunk_size = 5_000_000
        rg_size = 1_000_000

        # Adjust for small SF
        if sf <= 1:
            rg_size = 100_000
        elif sf <= 10:
            rg_size = 500_000

        # ── In-Memory Benchmark ──
        if not args.skip_inmemory:
            print(f"\n{'='*70}")
            print(f"  SF{sf} — IN-MEMORY ({li_rows:,} lineitem, {ord_rows:,} orders)")
            print(f"{'='*70}")

            print("\nStarting workers (in-memory mode)...")
            stop_workers()
            start_workers(worker_ports)

            print(f"\nGenerating SF{sf} in-memory data...")
            generate_inmemory_data(worker_ports, sf, chunk_size)

            print(f"\nRunning benchmark suite (in-memory, SF{sf})...")
            inmem_results = run_suite(f"inmem-SF{sf}", "prism_inmem", sf)
            all_results.update(inmem_results)

            # Save after each run
            with open(results_file, "w") as f:
                json.dump(all_results, f, indent=2)

        # ── Parquet Benchmark ──
        if not args.skip_parquet:
            print(f"\n{'='*70}")
            print(f"  SF{sf} — PARQUET ({li_rows:,} lineitem, {ord_rows:,} orders, rg_size={rg_size:,})")
            print(f"{'='*70}")

            print("\nStarting workers (Parquet mode)...")
            stop_workers()
            start_workers(worker_ports, data_dir=data_dir)

            print(f"\nGenerating SF{sf} sorted Parquet data...")
            generate_parquet_data(worker_ports, sf, data_dir, rg_size)

            print(f"\nRunning benchmark suite (Parquet, SF{sf})...")
            pq_results = run_suite(f"parquet-SF{sf}", "prism_parquet", sf)
            all_results.update(pq_results)

            # Save after each run
            with open(results_file, "w") as f:
                json.dump(all_results, f, indent=2)

    # ── Comparison Table ──
    print(f"\n\n{'='*70}")
    print("  COMPARISON: In-Memory vs Parquet")
    print(f"{'='*70}")

    for sf in scale_factors:
        print(f"\n  SF{sf}:")
        print(f"  {'Query':<14} {'InMem (ms)':>11} {'Parquet (ms)':>13} {'Speedup':>9}")
        print(f"  {'-'*49}")
        for qname in QUERY_ORDER:
            inmem_key = f"prism_inmem_sf{sf}_{qname}"
            pq_key = f"prism_parquet_sf{sf}_{qname}"
            inmem_ms = all_results.get(inmem_key, {}).get("ms", 0)
            pq_ms = all_results.get(pq_key, {}).get("ms", 0)
            if pq_ms > 0 and inmem_ms > 0:
                speedup = inmem_ms / pq_ms
                marker = " ***" if speedup > 1.5 else ""
                print(f"  {qname:<14} {inmem_ms:>11.0f} {pq_ms:>13.0f} {speedup:>8.2f}x{marker}")
            else:
                print(f"  {qname:<14} {inmem_ms:>11.0f} {pq_ms:>13.0f} {'N/A':>9}")

    # Save final results
    with open(results_file, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults saved to {results_file}")

    # Clean up
    stop_workers()


if __name__ == "__main__":
    main()
