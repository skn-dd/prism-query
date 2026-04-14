#!/usr/bin/env python3
"""Trino Benchmark Suite — SF10, REST API, server-side timing."""

import json, re, sys, time, urllib.request, urllib.error

SERVER = "http://localhost:8080"
WARMUP = 1
RUNS = 3

def submit_query(catalog, schema, sql):
    """Submit query via REST, poll to completion, return server-side elapsed ms."""
    data = sql.encode()
    req = urllib.request.Request(
        f"{SERVER}/v1/statement", data=data,
        headers={"X-Trino-User": "benchmark", "X-Trino-Catalog": catalog, "X-Trino-Schema": schema},
    )
    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read())

    query_id = body.get("id", "")
    next_uri = body.get("nextUri", "")

    while next_uri:
        time.sleep(0.05)
        req = urllib.request.Request(next_uri, headers={"X-Trino-User": "benchmark"})
        try:
            with urllib.request.urlopen(req) as resp:
                body = json.loads(resp.read())
            next_uri = body.get("nextUri", "")
        except urllib.error.HTTPError:
            break

    if "error" in body:
        raise RuntimeError(f"Query failed: {body['error'].get('message', 'unknown')}")

    time.sleep(0.2)
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
        return val
    return 0.0


def run_benchmark(name, catalog, schema, sql):
    print(f"  Running: {name}", flush=True)
    for _ in range(WARMUP):
        try: submit_query(catalog, schema, sql)
        except Exception as e: print(f"    Warmup error: {e}")

    times = []
    for i in range(RUNS):
        try:
            ms = submit_query(catalog, schema, sql)
            times.append(ms)
            print(f"    Run {i+1}: {ms:.2f}ms", flush=True)
        except Exception as e:
            print(f"    Run {i+1}: ERROR {e}")

    if not times:
        print(f"    ALL RUNS FAILED", flush=True)
        return 0, []
    times.sort()
    median = times[len(times)//2]
    print(f"    Median: {median:.2f}ms", flush=True)
    return median, times


# TPC-H (Trino unprefixed columns)
TPCH_Q1 = """
SELECT returnflag, linestatus,
       SUM(quantity), SUM(extendedprice),
       SUM(extendedprice * (1 - discount)),
       SUM(extendedprice * (1 - discount) * (1 + tax)),
       AVG(quantity), AVG(extendedprice), AVG(discount), COUNT(*)
FROM lineitem WHERE shipdate <= DATE '1998-09-02'
GROUP BY returnflag, linestatus ORDER BY returnflag, linestatus
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

# TPC-DS (standard prefixed columns)
TPCDS_Q3 = """
SELECT dt.d_year, item.i_brand_id, item.i_brand, SUM(ss_ext_sales_price)
FROM date_dim dt, store_sales, item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
  AND store_sales.ss_item_sk = item.i_item_sk
  AND item.i_manufact_id = 128 AND dt.d_moy = 11
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year, 4 DESC, item.i_brand_id LIMIT 100
"""

TPCDS_Q7 = """
SELECT i_item_id, AVG(ss_quantity), AVG(ss_list_price), AVG(ss_coupon_amt), AVG(ss_sales_price)
FROM store_sales, customer_demographics, date_dim, item, promotion
WHERE ss_sold_date_sk = d_date_sk AND ss_item_sk = i_item_sk
  AND ss_cdemo_sk = cd_demo_sk AND ss_promo_sk = p_promo_sk
  AND cd_gender = 'M' AND cd_marital_status = 'S' AND cd_education_status = 'College'
  AND (p_channel_email = 'N' OR p_channel_event = 'N') AND d_year = 2000
GROUP BY i_item_id ORDER BY i_item_id LIMIT 100
"""

TPCDS_Q55 = """
SELECT i_brand_id, i_brand, SUM(ss_ext_sales_price)
FROM date_dim, store_sales, item
WHERE d_date_sk = ss_sold_date_sk AND ss_item_sk = i_item_sk
  AND i_manager_id = 28 AND d_moy = 11 AND d_year = 1999
GROUP BY i_brand_id, i_brand ORDER BY 3 DESC, i_brand_id LIMIT 100
"""

TPCDS_Q96 = """
SELECT COUNT(*)
FROM store_sales, household_demographics, time_dim, store
WHERE ss_sold_time_sk = time_dim.t_time_sk
  AND ss_hdemo_sk = household_demographics.hd_demo_sk
  AND ss_store_sk = s_store_sk
  AND time_dim.t_hour = 20 AND time_dim.t_minute >= 30
  AND household_demographics.hd_dep_count = 7
  AND store.s_store_name = 'ese'
ORDER BY COUNT(*) LIMIT 100
"""


def main():
    results = {}
    print("=" * 70)
    print("  Trino Benchmark Suite — SF10 (server-side timing)")
    print("=" * 70)

    print("\n━━━ TPC-H SF10 (60M lineitem rows) ━━━")
    for name, sql in [("q1", TPCH_Q1), ("q3", TPCH_Q3), ("q6", TPCH_Q6)]:
        key = f"tpch_{name}_sf10"
        median, _ = run_benchmark(key, "tpch", "sf10", sql)
        results[key] = median

    print("\n━━━ TPC-DS SF10 (~29M store_sales rows) ━━━")
    for name, sql in [("q3", TPCDS_Q3), ("q7", TPCDS_Q7), ("q55", TPCDS_Q55), ("q96", TPCDS_Q96)]:
        key = f"tpcds_{name}_sf10"
        median, _ = run_benchmark(key, "tpcds", "sf10", sql)
        results[key] = median

    print("\n" + "=" * 70)
    print("  RESULTS SUMMARY — SF10 (server-side ms)")
    print("=" * 70)
    for k, v in results.items():
        print(f"  {k:40s} {v:>10.2f} ms")

    # Also print SF1 comparison
    try:
        with open("/Users/ec2-user/code/prism/trino-bench/results-rest.json") as f:
            sf1 = json.load(f)
        print("\n  SF1 → SF10 Scaling:")
        for name in ["tpch_q1", "tpch_q3", "tpch_q6"]:
            s1 = sf1.get(f"{name}_sf1", 0)
            s10 = results.get(f"{name}_sf10", 0)
            if s1 > 0 and s10 > 0:
                print(f"    {name}: {s1:.0f}ms → {s10:.0f}ms ({s10/s1:.1f}x for 10x data)")
    except: pass

    with open("/Users/ec2-user/code/prism/trino-bench/results-sf10.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to trino-bench/results-sf10.json")


if __name__ == "__main__":
    main()
