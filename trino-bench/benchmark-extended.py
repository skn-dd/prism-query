#!/usr/bin/env python3
"""Extended Prism vs Trino Benchmark — matches TPC-H Q1/Q3/Q6 + join + agg patterns."""

import json, re, sys, time, urllib.request, urllib.error

SERVER = "http://localhost:8080"
WARMUP = 1
RUNS = 4

def submit_query(catalog, schema, sql):
    """Submit query via REST, poll to completion, return server-side elapsed ms + row count."""
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
    rows = 0
    for i in range(RUNS):
        try:
            ms, r = submit_query(catalog, schema, sql)
            times.append(ms)
            rows = r
            print(f"    Run {i+1}: {ms:.2f}ms ({r} rows)", flush=True)
        except Exception as e:
            print(f"    Run {i+1}: ERROR {e}")

    if not times:
        print(f"    ALL RUNS FAILED", flush=True)
        return 0, 0

    times.sort()
    median = times[len(times)//2]
    print(f"    Median: {median:.2f}ms", flush=True)
    return median, rows


# ── TPC-H Queries — Trino tpch catalog (unprefixed columns) ──

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

TPCH_Q3 = """
SELECT l.orderkey, SUM(l.extendedprice * (1 - l.discount)) as revenue,
       o.orderdate, o.shippriority
FROM customer c, orders o, lineitem l
WHERE c.mktsegment = 'BUILDING' AND c.custkey = o.custkey
  AND l.orderkey = o.orderkey
  AND o.orderdate < DATE '1995-03-15' AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate LIMIT 10
"""

TPCH_Q6 = """
SELECT SUM(extendedprice * discount) as revenue
FROM lineitem
WHERE shipdate >= DATE '1994-01-01' AND shipdate < DATE '1995-01-01'
  AND discount BETWEEN 0.05 AND 0.07 AND quantity < 24
"""

TPCH_SCAN = "SELECT COUNT(*) FROM lineitem"
TPCH_FILTER = "SELECT COUNT(*) FROM lineitem WHERE quantity > 25 AND discount < 0.05"
TPCH_AGG = """
SELECT returnflag, SUM(extendedprice), AVG(quantity), COUNT(*)
FROM lineitem GROUP BY returnflag
"""

# ── Prism Queries — prefixed columns matching datagen.rs ──

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

# Q3-equivalent: 2-way join (lineitem x orders) — no customer table in Prism
PRISM_JOIN = """
SELECT o_orderstatus,
       SUM(l_extendedprice * (1 - l_discount)) as revenue,
       COUNT(*) as cnt
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
GROUP BY o_orderstatus
ORDER BY revenue DESC
"""

PRISM_Q6 = """
SELECT SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24
"""

PRISM_SCAN = "SELECT COUNT(*) FROM lineitem"
PRISM_FILTER = "SELECT COUNT(*) FROM lineitem WHERE l_quantity > 25 AND l_discount < 0.05"
PRISM_AGG = """
SELECT l_returnflag, SUM(l_extendedprice), AVG(l_quantity), COUNT(*)
FROM lineitem GROUP BY l_returnflag
"""

# Orders-only queries
TPCH_ORDERS_AGG = """
SELECT orderstatus, COUNT(*), SUM(totalprice)
FROM orders GROUP BY orderstatus
"""
PRISM_ORDERS_AGG = """
SELECT o_orderstatus, COUNT(*), SUM(o_totalprice)
FROM orders GROUP BY o_orderstatus
"""

# TopN
TPCH_TOPN = """
SELECT orderkey, extendedprice, quantity
FROM lineitem ORDER BY extendedprice DESC LIMIT 10
"""
PRISM_TOPN = """
SELECT l_orderkey, l_extendedprice, l_quantity
FROM lineitem ORDER BY l_extendedprice DESC LIMIT 10
"""

# Multi-agg (heavy aggregation)
TPCH_MULTI_AGG = """
SELECT returnflag, linestatus,
       SUM(quantity), SUM(extendedprice), MIN(discount), MAX(discount),
       AVG(quantity), AVG(extendedprice), COUNT(*)
FROM lineitem GROUP BY returnflag, linestatus
"""
PRISM_MULTI_AGG = """
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity), SUM(l_extendedprice), MIN(l_discount), MAX(l_discount),
       AVG(l_quantity), AVG(l_extendedprice), COUNT(*)
FROM lineitem GROUP BY l_returnflag, l_linestatus
"""

# ── Trino tpch join baseline (2-way join matching Prism's join query) ──
TPCH_JOIN = """
SELECT orderstatus,
       SUM(extendedprice * (1 - discount)) as revenue,
       COUNT(*) as cnt
FROM lineitem JOIN orders ON orderkey = orderkey
GROUP BY orderstatus
ORDER BY revenue DESC
"""
# Note: tpch catalog uses "orderkey" for both tables — implicit join column

def main():
    results = {}

    print("=" * 70)
    print("  Extended Prism vs Trino Benchmark")
    print("=" * 70)

    # ── Trino Baseline ──
    print("\n--- Trino Baseline (tpch.sf1) ---")
    trino_queries = [
        ("trino_scan",      TPCH_SCAN),
        ("trino_filter",    TPCH_FILTER),
        ("trino_agg",       TPCH_AGG),
        ("trino_q1",        TPCH_Q1),
        ("trino_q6",        TPCH_Q6),
        ("trino_q3",        TPCH_Q3),
        ("trino_orders_agg",TPCH_ORDERS_AGG),
        ("trino_multi_agg", TPCH_MULTI_AGG),
        ("trino_topn",      TPCH_TOPN),
    ]
    for name, sql in trino_queries:
        median, rows = run_benchmark(name, "tpch", "sf1", sql)
        results[name] = {"ms": median, "rows": rows}

    # ── Prism ──
    print("\n--- Prism (prism.tpch) ---")
    prism_queries = [
        ("prism_scan",      PRISM_SCAN),
        ("prism_filter",    PRISM_FILTER),
        ("prism_agg",       PRISM_AGG),
        ("prism_q1",        PRISM_Q1),
        ("prism_q6",        PRISM_Q6),
        ("prism_join",      PRISM_JOIN),
        ("prism_orders_agg",PRISM_ORDERS_AGG),
        ("prism_multi_agg", PRISM_MULTI_AGG),
        ("prism_topn",      PRISM_TOPN),
    ]
    for name, sql in prism_queries:
        median, rows = run_benchmark(name, "prism", "tpch", sql)
        results[name] = {"ms": median, "rows": rows}

    # ── Comparison ──
    print("\n" + "=" * 70)
    print("  COMPARISON")
    print("=" * 70)
    print(f"  {'Query':<18} {'Trino (ms)':>11} {'Prism (ms)':>11} {'Speedup':>9} {'Rows':>8} {'Pushdown':>12}")
    print("  " + "-" * 71)

    pairs = [
        ("scan",       "scan",       "Full"),
        ("filter",     "filter",     "Full"),
        ("agg",        "agg",        "Full"),
        ("q1",         "q1",         "Full"),
        ("q6",         "q6",         "Full"),
        ("orders_agg", "orders_agg", "Full"),
        ("multi_agg",  "multi_agg",  "Full"),
        ("q3",         "join",       "Scan only"),
        ("topn",       "topn",       "Scan only"),
    ]
    for tname, pname, pushdown in pairs:
        tkey = f"trino_{tname}"
        pkey = f"prism_{pname}"
        tms = results.get(tkey, {}).get("ms", 0)
        pms = results.get(pkey, {}).get("ms", 0)
        rows = results.get(pkey, {}).get("rows", 0)
        speedup = tms / pms if pms > 0 else 0
        print(f"  {tname:<18} {tms:>11.0f} {pms:>11.0f} {speedup:>8.1f}x {rows:>8} {pushdown:>12}")

    # Save
    with open("/Users/ec2-user/code/prism/trino-bench/results-extended.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to trino-bench/results-extended.json")


if __name__ == "__main__":
    main()
