#!/usr/bin/env python3
"""Prism-through-Trino Benchmark — same queries, prism catalog, server-side timing."""

import json, re, sys, time, urllib.request, urllib.error

SERVER = "http://localhost:8080"
WARMUP = 1
RUNS = 4

def submit_query(catalog, schema, sql):
    """Submit query via REST, poll to completion, return server-side elapsed ms."""
    data = sql.encode()
    req = urllib.request.Request(
        f"{SERVER}/v1/statement",
        data=data,
        headers={
            "X-Trino-User": "benchmark",
            "X-Trino-Catalog": catalog,
            "X-Trino-Schema": schema,
        },
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
        err = body["error"]
        raise RuntimeError(f"Query failed: {err.get('message', 'unknown error')}")

    time.sleep(0.2)
    req = urllib.request.Request(
        f"{SERVER}/v1/query/{query_id}",
        headers={"X-Trino-User": "benchmark"},
    )
    with urllib.request.urlopen(req) as resp:
        stats = json.loads(resp.read())

    state = stats.get("state", "")
    if state == "FAILED":
        fi = stats.get("failureInfo", {})
        raise RuntimeError(f"Query FAILED: {fi.get('message', 'unknown')}")

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
    """Run warmup + measured runs, return median."""
    print(f"  Running: {name}", flush=True)

    for _ in range(WARMUP):
        try:
            submit_query(catalog, schema, sql)
        except Exception as e:
            print(f"    Warmup error: {e}")

    times = []
    for i in range(RUNS):
        try:
            ms, rows = submit_query(catalog, schema, sql)
            times.append(ms)
            print(f"    Run {i+1}: {ms:.2f}ms ({rows} rows)", flush=True)
        except Exception as e:
            print(f"    Run {i+1}: ERROR {e}")

    if not times:
        print(f"    ALL RUNS FAILED", flush=True)
        return 0, []

    times.sort()
    median = times[len(times)//2]
    print(f"    Median: {median:.2f}ms", flush=True)
    return median, times


# ── Prism TPC-H Queries (prefixed column names matching datagen.rs) ──

PRISM_Q1 = """
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity) as sum_qty,
       SUM(l_extendedprice) as sum_base_price,
       SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
       AVG(l_quantity) as avg_qty, AVG(l_extendedprice) as avg_price,
       AVG(l_discount) as avg_disc, COUNT(*) as count_order
FROM lineitem
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
"""

PRISM_Q6 = """
SELECT SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24
"""

PRISM_SCAN = """
SELECT count(*) FROM lineitem
"""

PRISM_FILTER = """
SELECT count(*) FROM lineitem
WHERE l_quantity > 25 AND l_discount < 0.05
"""

PRISM_AGG = """
SELECT l_returnflag, sum(l_extendedprice), avg(l_quantity), count(*)
FROM lineitem
GROUP BY l_returnflag
"""

PRISM_PROJECT = """
SELECT l_orderkey, l_quantity, l_extendedprice
FROM lineitem LIMIT 1000
"""

# Also run the SAME TPC-H queries through the standard tpch catalog for comparison
TPCH_Q1 = """
SELECT returnflag, linestatus,
       SUM(quantity) as sum_qty,
       SUM(extendedprice) as sum_base_price,
       SUM(extendedprice * (1 - discount)) as sum_disc_price,
       SUM(extendedprice * (1 - discount) * (1 + tax)) as sum_charge,
       AVG(quantity) as avg_qty, AVG(extendedprice) as avg_price,
       AVG(discount) as avg_disc, COUNT(*) as count_order
FROM lineitem WHERE shipdate <= DATE '1998-09-02'
GROUP BY returnflag, linestatus
ORDER BY returnflag, linestatus
"""

TPCH_Q6 = """
SELECT SUM(extendedprice * discount) as revenue
FROM lineitem
WHERE shipdate >= DATE '1994-01-01' AND shipdate < DATE '1995-01-01'
  AND discount BETWEEN 0.05 AND 0.07 AND quantity < 24
"""

TPCH_SCAN = "SELECT count(*) FROM lineitem"
TPCH_FILTER = "SELECT count(*) FROM lineitem WHERE quantity > 25 AND discount < 0.05"
TPCH_AGG = """
SELECT returnflag, sum(extendedprice), avg(quantity), count(*)
FROM lineitem GROUP BY returnflag
"""


def main():
    prism_results = {}
    trino_results = {}

    print("=" * 70)
    print("  Prism vs Trino Benchmark — REST API (server-side timing)")
    print("=" * 70)
    print()

    # ── Trino baseline (tpch catalog, SF1) ──
    print("━━━ Trino Baseline (tpch.sf1) ━━━")
    for name, sql in [
        ("scan", TPCH_SCAN), ("filter", TPCH_FILTER), ("agg", TPCH_AGG),
        ("q1", TPCH_Q1), ("q6", TPCH_Q6),
    ]:
        key = f"trino_{name}"
        median, _ = run_benchmark(key, "tpch", "sf1", sql)
        trino_results[key] = median

    # ── Prism (prism catalog) ──
    print("\n━━━ Prism (prism.tpch) ━━━")
    for name, sql in [
        ("scan", PRISM_SCAN), ("filter", PRISM_FILTER), ("agg", PRISM_AGG),
        ("q1", PRISM_Q1), ("q6", PRISM_Q6), ("project", PRISM_PROJECT),
    ]:
        key = f"prism_{name}"
        median, _ = run_benchmark(key, "prism", "tpch", sql)
        prism_results[key] = median

    # ── Comparison ──
    print("\n" + "=" * 70)
    print("  COMPARISON: Trino (JVM) vs Prism (Rust workers)")
    print("=" * 70)
    print(f"  {'Query':<20} {'Trino (ms)':>12} {'Prism (ms)':>12} {'Speedup':>10}")
    print("  " + "-" * 56)

    pairs = [("scan", "scan"), ("filter", "filter"), ("agg", "agg"),
             ("q1", "q1"), ("q6", "q6")]
    for tname, pname in pairs:
        tkey = f"trino_{tname}"
        pkey = f"prism_{pname}"
        tms = trino_results.get(tkey, 0)
        pms = prism_results.get(pkey, 0)
        speedup = tms / pms if pms > 0 else 0
        marker = " ***" if speedup > 2 else ""
        print(f"  {tname:<20} {tms:>12.2f} {pms:>12.2f} {speedup:>9.1f}x{marker}")

    all_results = {**trino_results, **prism_results}
    with open("/Users/ec2-user/code/prism/trino-bench/results-prism-vs-trino.json", "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults saved to trino-bench/results-prism-vs-trino.json")


if __name__ == "__main__":
    main()
