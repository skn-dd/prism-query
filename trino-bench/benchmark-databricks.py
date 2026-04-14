#!/usr/bin/env python3
"""
Databricks TPC-H Benchmark -- SF1/SF10/SF100
Same 9 queries as the Prism/Trino benchmark suite.
Methodology: 1 warmup + 4 measured runs, median reported.

Data: Generated as Delta tables from samples.tpch (~SF5 = 30M lineitem).
  SF1:   6M lineitem /  1.5M orders  (subset of samples)
  SF10:  60M lineitem / 15M orders   (2x replication of samples)
  SF100: 600M lineitem / 150M orders (20x replication of samples)
"""

from databricks import sql
import os
import time
import json
import statistics
import sys

SERVER_HOSTNAME = os.environ.get("DATABRICKS_HOST", "adb-6702566181256084.4.azuredatabricks.net")
HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "sql/protocolv1/o/6702566181256084/0414-202742-5l85p28y")
ACCESS_TOKEN = os.environ["DATABRICKS_TOKEN"]  # required: set via environment variable

QUERIES = {
    "scan": "SELECT COUNT(*) FROM {lineitem}",
    "filter": "SELECT COUNT(*) FROM {lineitem} WHERE l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24",
    "agg": "SELECT l_returnflag, SUM(l_quantity), AVG(l_extendedprice), COUNT(*) FROM {lineitem} GROUP BY l_returnflag",
    "q1": """SELECT l_returnflag, l_linestatus,
        SUM(l_quantity), SUM(l_extendedprice),
        SUM(l_extendedprice * (1 - l_discount)),
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
        AVG(l_quantity), AVG(l_extendedprice),
        AVG(l_discount), COUNT(*)
    FROM {lineitem}
    GROUP BY l_returnflag, l_linestatus""",
    "q6": """SELECT SUM(l_extendedprice * l_discount) AS revenue
    FROM {lineitem}
    WHERE l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24""",
    "orders_agg": "SELECT o_orderstatus, SUM(o_totalprice), COUNT(*) FROM {orders} GROUP BY o_orderstatus",
    "multi_agg": """SELECT l_returnflag, l_linestatus,
        SUM(l_quantity), SUM(l_extendedprice), SUM(l_discount),
        AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount),
        COUNT(*)
    FROM {lineitem}
    GROUP BY l_returnflag, l_linestatus""",
    "topn": "SELECT l_orderkey, l_extendedprice FROM {lineitem} ORDER BY l_extendedprice DESC LIMIT 10",
    "join": """SELECT o_orderstatus,
        SUM(l_extendedprice * (1 - l_discount)) AS revenue, COUNT(*)
    FROM {lineitem} JOIN {orders} ON l_orderkey = o_orderkey
    GROUP BY o_orderstatus""",
}

NUM_WARMUP = 1
NUM_RUNS = 4

# samples.tpch row counts (SF5)
SAMPLES_LINEITEM = 29_999_795
SAMPLES_ORDERS   =  7_500_000

# Target rows per SF
LINEITEM_PER_SF = 6_001_215
ORDERS_PER_SF   = 1_500_000


def connect():
    print("Connecting to Databricks...", flush=True)
    conn = sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=ACCESS_TOKEN,
    )
    print("Connected.", flush=True)
    return conn


def run_query(cursor, query):
    start = time.time()
    cursor.execute(query)
    rows = cursor.fetchall()
    elapsed = (time.time() - start) * 1000
    return elapsed, len(rows)


def check_table(cursor, fqn):
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {fqn}")
        return cursor.fetchall()[0][0]
    except:
        return None


def generate_data(cursor, sf):
    """Generate TPC-H data at the given scale factor as Delta tables."""
    db = f"tpch_sf{sf}"
    li = f"{db}.lineitem"
    od = f"{db}.orders"

    target_li = sf * LINEITEM_PER_SF
    target_od = sf * ORDERS_PER_SF

    # Check if already generated
    existing = check_table(cursor, li)
    if existing is not None and existing >= target_li * 0.95:
        print(f"  {li} already has {existing:,} rows (target {target_li:,}), reusing", flush=True)
        existing_o = check_table(cursor, od)
        print(f"  {od} already has {existing_o:,} rows", flush=True)
        return (li, od)

    print(f"\n  Generating SF{sf}: {target_li:,} lineitem + {target_od:,} orders", flush=True)
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db}")

    if sf == 1:
        # Subset of samples.tpch
        print(f"  Creating {li} (subset of samples, {target_li:,} rows)...", flush=True)
        start = time.time()
        cursor.execute(f"""
            CREATE OR REPLACE TABLE {li} USING DELTA AS
            SELECT * FROM samples.tpch.lineitem LIMIT {target_li}
        """)
        elapsed = time.time() - start
        count = check_table(cursor, li)
        print(f"  Created {li}: {count:,} rows in {elapsed:.0f}s", flush=True)

        print(f"  Creating {od} (subset, {target_od:,} rows)...", flush=True)
        start = time.time()
        cursor.execute(f"""
            CREATE OR REPLACE TABLE {od} USING DELTA AS
            SELECT * FROM samples.tpch.orders LIMIT {target_od}
        """)
        elapsed = time.time() - start
        count = check_table(cursor, od)
        print(f"  Created {od}: {count:,} rows in {elapsed:.0f}s", flush=True)

    elif sf == 10:
        # 2x replication of samples (~30M * 2 = 60M)
        print(f"  Creating {li} (2x samples, ~60M rows)...", flush=True)
        start = time.time()
        cursor.execute(f"""
            CREATE OR REPLACE TABLE {li} USING DELTA AS
            SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber,
                   l_quantity, l_extendedprice, l_discount, l_tax,
                   l_returnflag, l_linestatus,
                   l_shipdate, l_commitdate, l_receiptdate,
                   l_shipinstruct, l_shipmode, l_comment
            FROM samples.tpch.lineitem
            UNION ALL
            SELECT l_orderkey + {SAMPLES_LINEITEM}, l_partkey, l_suppkey, l_linenumber,
                   l_quantity, l_extendedprice, l_discount, l_tax,
                   l_returnflag, l_linestatus,
                   l_shipdate, l_commitdate, l_receiptdate,
                   l_shipinstruct, l_shipmode, l_comment
            FROM samples.tpch.lineitem
        """)
        elapsed = time.time() - start
        count = check_table(cursor, li)
        print(f"  Created {li}: {count:,} rows in {elapsed:.0f}s", flush=True)

        print(f"  Creating {od} (2x samples, ~15M rows)...", flush=True)
        start = time.time()
        cursor.execute(f"""
            CREATE OR REPLACE TABLE {od} USING DELTA AS
            SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice,
                   o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment
            FROM samples.tpch.orders
            UNION ALL
            SELECT o_orderkey + {SAMPLES_ORDERS}, o_custkey, o_orderstatus, o_totalprice,
                   o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment
            FROM samples.tpch.orders
        """)
        elapsed = time.time() - start
        count = check_table(cursor, od)
        print(f"  Created {od}: {count:,} rows in {elapsed:.0f}s", flush=True)

    elif sf == 100:
        # 20x replication of samples (~30M * 20 = 600M)
        # Do in batches of 5x to avoid too-large queries
        print(f"  Creating {li} (20x samples, ~600M rows) in 4 batches...", flush=True)
        total_start = time.time()

        for batch_num in range(4):  # 4 batches of 5x
            base_offset = batch_num * 5 * SAMPLES_LINEITEM
            unions = " UNION ALL ".join([
                f"""SELECT l_orderkey + {base_offset + i * SAMPLES_LINEITEM} AS l_orderkey,
                       l_partkey, l_suppkey, l_linenumber,
                       l_quantity, l_extendedprice, l_discount, l_tax,
                       l_returnflag, l_linestatus,
                       l_shipdate, l_commitdate, l_receiptdate,
                       l_shipinstruct, l_shipmode, l_comment
                FROM samples.tpch.lineitem"""
                for i in range(5)
            ])

            start = time.time()
            if batch_num == 0:
                cursor.execute(f"CREATE OR REPLACE TABLE {li} USING DELTA AS {unions}")
            else:
                cursor.execute(f"INSERT INTO {li} {unions}")
            elapsed = time.time() - start
            print(f"    Batch {batch_num + 1}/4: +{5 * SAMPLES_LINEITEM:,} rows in {elapsed:.0f}s", flush=True)

        total = time.time() - total_start
        count = check_table(cursor, li)
        print(f"  Created {li}: {count:,} rows in {total:.0f}s total", flush=True)

        print(f"  Creating {od} (20x samples, ~150M rows) in 4 batches...", flush=True)
        total_start = time.time()

        for batch_num in range(4):
            base_offset = batch_num * 5 * SAMPLES_ORDERS
            unions = " UNION ALL ".join([
                f"""SELECT o_orderkey + {base_offset + i * SAMPLES_ORDERS} AS o_orderkey,
                       o_custkey, o_orderstatus, o_totalprice,
                       o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment
                FROM samples.tpch.orders"""
                for i in range(5)
            ])

            start = time.time()
            if batch_num == 0:
                cursor.execute(f"CREATE OR REPLACE TABLE {od} USING DELTA AS {unions}")
            else:
                cursor.execute(f"INSERT INTO {od} {unions}")
            elapsed = time.time() - start
            print(f"    Batch {batch_num + 1}/4: +{5 * SAMPLES_ORDERS:,} rows in {elapsed:.0f}s", flush=True)

        total = time.time() - total_start
        count = check_table(cursor, od)
        print(f"  Created {od}: {count:,} rows in {total:.0f}s total", flush=True)

    return (li, od)


def benchmark_sf(cursor, sf, lineitem, orders, results):
    print(f"\n{'='*70}", flush=True)
    print(f"  Databricks Benchmark -- SF{sf}", flush=True)
    print(f"  lineitem: {lineitem}", flush=True)
    print(f"  orders:   {orders}", flush=True)
    print(f"{'='*70}", flush=True)

    for name, template in QUERIES.items():
        query = template.format(lineitem=lineitem, orders=orders)
        key = f"databricks_sf{sf}_{name}"
        print(f"\n  Running: {name}", flush=True)

        # Warmup
        try:
            elapsed, rows = run_query(cursor, query)
            print(f"    Warmup: {elapsed:,.0f}ms ({rows} rows)", flush=True)
        except Exception as e:
            print(f"    Warmup ERROR: {e}", flush=True)
            results[key] = {"ms": None, "rows": 0, "error": str(e)[:200]}
            continue

        # Measured runs
        times = []
        row_count = 0
        for i in range(NUM_RUNS):
            try:
                elapsed, rows = run_query(cursor, query)
                times.append(elapsed)
                row_count = rows
                print(f"    Run {i+1}: {elapsed:,.0f}ms ({rows} rows)", flush=True)
            except Exception as e:
                print(f"    Run {i+1}: ERROR {e}", flush=True)

        if times:
            median = statistics.median(times)
            print(f"    Median: {median:,.0f}ms", flush=True)
            results[key] = {"ms": round(median, 2), "rows": row_count}
        else:
            results[key] = {"ms": None, "rows": 0, "error": "all runs failed"}


def print_comparison(results):
    prism = {
        1:   {"scan": 158, "filter": 158, "agg": 160, "q1": 254, "q6": 158,
              "orders_agg": 156, "multi_agg": 230, "topn": 196, "join": 179},
        10:  {"scan": 491, "filter": 368, "agg": 962, "q1": 2000, "q6": 208,
              "orders_agg": 253, "multi_agg": 1690, "topn": 1160, "join": 1020},
        100: {"scan": 4310, "filter": 3110, "agg": 9000, "q1": 19370, "q6": 1880,
              "orders_agg": 1940, "multi_agg": 14780, "topn": 11460, "join": 9570},
    }
    trino = {
        1:   {"scan": 188, "filter": 181, "agg": 248, "q1": 308, "q6": 201,
              "orders_agg": 162, "multi_agg": 319, "topn": 271, "join": 668},
        10:  {"scan": 1210, "filter": 1290, "agg": 1820, "q1": 2080, "q6": 1370,
              "orders_agg": 678, "multi_agg": 2160, "topn": 1850, "join": 5100},
        100: {"scan": 12370, "filter": 12260, "agg": 17870, "q1": 20650, "q6": 12850,
              "orders_agg": 6080, "multi_agg": 21620, "topn": 17600, "join": None},
    }

    for sf in [1, 10, 100]:
        db_results = {k.replace(f"databricks_sf{sf}_", ""): v
                      for k, v in results.items()
                      if k.startswith(f"databricks_sf{sf}_")}
        if not db_results:
            continue

        print(f"\n{'='*95}", flush=True)
        print(f"  SF{sf} COMPARISON: Databricks vs Prism vs Trino", flush=True)
        print(f"{'='*95}", flush=True)
        print(f"  {'Query':<12} {'Databricks':>12} {'Prism':>12} {'Trino':>12} {'DB/Prism':>10} {'DB/Trino':>10}", flush=True)
        print(f"  {'-'*12} {'-'*12} {'-'*12} {'-'*12} {'-'*10} {'-'*10}", flush=True)

        for name in QUERIES:
            db_ms = db_results.get(name, {}).get("ms")
            p_ms = prism.get(sf, {}).get(name)
            t_ms = trino.get(sf, {}).get(name)

            db_s = f"{db_ms:,.0f}ms" if db_ms else "N/A"
            p_s  = f"{p_ms:,.0f}ms" if p_ms else "N/A"
            t_s  = f"{t_ms:,.0f}ms" if t_ms else "OOM"

            if db_ms and p_ms:
                r = p_ms / db_ms
                dbp = f"{r:.1f}x" if r >= 1 else f"1/{1/r:.1f}x"
            else:
                dbp = "-"

            if db_ms and t_ms:
                r = t_ms / db_ms
                dbt = f"{r:.1f}x" if r >= 1 else f"1/{1/r:.1f}x"
            else:
                dbt = "-"

            print(f"  {name:<12} {db_s:>12} {p_s:>12} {t_s:>12} {dbp:>10} {dbt:>10}", flush=True)


def main():
    conn = connect()
    cursor = conn.cursor()

    # Cluster info
    try:
        cursor.execute("SELECT current_catalog(), current_database()")
        row = cursor.fetchall()[0]
        print(f"Catalog: {row[0]}, Database: {row[1]}", flush=True)
    except Exception as e:
        print(f"Cluster info: {e}", flush=True)

    # Load any existing results (resume after partial run)
    results = {}
    try:
        with open("trino-bench/results-databricks.json") as f:
            results = json.load(f)
        print(f"Loaded {len(results)} existing results", flush=True)
    except FileNotFoundError:
        pass

    # Generate and benchmark each scale factor
    for sf in [1, 10, 100]:
        # Skip if we already have all 9 results for this SF
        sf_keys = [k for k in results if k.startswith(f"databricks_sf{sf}_") and results[k].get("ms") is not None]
        if len(sf_keys) >= len(QUERIES):
            print(f"\n--- SF{sf}: already have {len(sf_keys)} results, skipping ---", flush=True)
            continue

        print(f"\n--- Preparing SF{sf} ---", flush=True)
        tables = generate_data(cursor, sf)
        if tables:
            lineitem, orders = tables
            benchmark_sf(cursor, sf, lineitem, orders, results)
        else:
            print(f"  Failed to prepare SF{sf} data, skipping", flush=True)

        # Save intermediate results
        with open("trino-bench/results-databricks.json", "w") as f:
            json.dump(results, f, indent=2)
        print(f"\n  Intermediate results saved.", flush=True)

    # Final save
    with open("trino-bench/results-databricks.json", "w") as f:
        json.dump(results, f, indent=2)

    # Print comparison
    print_comparison(results)

    # Summary
    print(f"\n{'='*70}", flush=True)
    print(f"  ALL DATABRICKS RESULTS", flush=True)
    print(f"{'='*70}", flush=True)
    for key, val in sorted(results.items()):
        ms = val.get("ms")
        if ms:
            print(f"  {key:<35} {ms:>10,.0f}ms  ({val.get('rows', 0)} rows)", flush=True)
        else:
            print(f"  {key:<35} {'FAILED':>10}  {val.get('error', '')[:50]}", flush=True)

    cursor.close()
    conn.close()
    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
