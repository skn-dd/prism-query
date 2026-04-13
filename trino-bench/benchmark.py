#!/usr/bin/env python3
"""Trino Benchmark Suite — REST API, server-side timing."""

import json, re, sys, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    # Poll until complete
    while next_uri:
        time.sleep(0.05)
        req = urllib.request.Request(next_uri, headers={"X-Trino-User": "benchmark"})
        try:
            with urllib.request.urlopen(req) as resp:
                body = json.loads(resp.read())
            next_uri = body.get("nextUri", "")
        except urllib.error.HTTPError:
            break

    # Check for query error in final poll response
    if "error" in body:
        err = body["error"]
        raise RuntimeError(f"Query failed: {err.get('message', 'unknown error')}")

    # Fetch query stats
    time.sleep(0.2)
    req = urllib.request.Request(
        f"{SERVER}/v1/query/{query_id}",
        headers={"X-Trino-User": "benchmark"},
    )
    with urllib.request.urlopen(req) as resp:
        stats = json.loads(resp.read())

    # Verify query succeeded
    state = stats.get("state", "")
    if state == "FAILED":
        fi = stats.get("failureInfo", {})
        raise RuntimeError(f"Query FAILED: {fi.get('message', 'unknown')}")

    qs = stats.get("queryStats", {})
    elapsed_str = qs.get("elapsedTime", "0ms")

    # Parse duration: "93.10ms", "1.23s", "2.50m"
    m = re.match(r"([0-9.]+)(ms|s|m|h)", elapsed_str)
    if m:
        val, unit = float(m.group(1)), m.group(2)
        if unit == "s": val *= 1000
        elif unit == "m": val *= 60000
        elif unit == "h": val *= 3600000
        return val
    return 0.0


def run_benchmark(name, catalog, schema, sql):
    """Run warmup + measured runs, return median."""
    print(f"  Running: {name}", flush=True)

    # Warmup
    for _ in range(WARMUP):
        try:
            submit_query(catalog, schema, sql)
        except Exception as e:
            print(f"    Warmup error: {e}")

    # Measured
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


# ── Queries ──────────────────────────────────────────────────────────────
# NOTE: Trino's TPC-H connector uses UNPREFIXED column names
#       (e.g., "shipdate" not "l_shipdate", "custkey" not "c_custkey")
# TPC-DS connector keeps standard prefixed names (ss_, i_, d_, etc.)

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

TPCDS_Q3 = """
SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,
       SUM(ss_ext_sales_price) sum_agg
FROM date_dim dt, store_sales, item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
  AND store_sales.ss_item_sk = item.i_item_sk
  AND item.i_manufact_id = 128 AND dt.d_moy = 11
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year, sum_agg DESC, brand_id LIMIT 100
"""

TPCDS_Q7 = """
SELECT i_item_id, AVG(ss_quantity) agg1, AVG(ss_list_price) agg2,
       AVG(ss_coupon_amt) agg3, AVG(ss_sales_price) agg4
FROM store_sales, customer_demographics, date_dim, item, promotion
WHERE ss_sold_date_sk = d_date_sk AND ss_item_sk = i_item_sk
  AND ss_cdemo_sk = cd_demo_sk AND ss_promo_sk = p_promo_sk
  AND cd_gender = 'M' AND cd_marital_status = 'S'
  AND cd_education_status = 'College'
  AND (p_channel_email = 'N' OR p_channel_event = 'N') AND d_year = 2000
GROUP BY i_item_id ORDER BY i_item_id LIMIT 100
"""

TPCDS_Q19 = """
SELECT i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
       SUM(ss_ext_sales_price) ext_price
FROM date_dim, store_sales, item, customer, customer_address, store
WHERE d_date_sk = ss_sold_date_sk AND ss_item_sk = i_item_sk
  AND i_manager_id = 8 AND d_moy = 11 AND d_year = 1998
  AND ss_customer_sk = c_customer_sk AND c_current_addr_sk = ca_address_sk
  AND SUBSTRING(ca_zip, 1, 5) <> SUBSTRING(s_zip, 1, 5)
  AND ss_store_sk = s_store_sk
GROUP BY i_brand_id, i_brand, i_manufact_id, i_manufact
ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact LIMIT 100
"""

TPCDS_Q27 = """
SELECT i_item_id, s_state, GROUPING(s_state) g_state,
       AVG(ss_quantity) agg1, AVG(ss_list_price) agg2,
       AVG(ss_coupon_amt) agg3, AVG(ss_sales_price) agg4
FROM store_sales, customer_demographics, date_dim, store, item
WHERE ss_sold_date_sk = d_date_sk AND ss_item_sk = i_item_sk
  AND ss_store_sk = s_store_sk AND ss_cdemo_sk = cd_demo_sk
  AND cd_gender = 'M' AND cd_marital_status = 'S'
  AND cd_education_status = 'College' AND d_year = 2002
  AND s_state IN ('TN')
GROUP BY ROLLUP(i_item_id, s_state)
ORDER BY i_item_id, s_state LIMIT 100
"""

TPCDS_Q43 = """
SELECT s_store_name, s_store_id,
       SUM(CASE WHEN d_day_name='Sunday' THEN ss_sales_price ELSE null END) sun_sales,
       SUM(CASE WHEN d_day_name='Monday' THEN ss_sales_price ELSE null END) mon_sales,
       SUM(CASE WHEN d_day_name='Tuesday' THEN ss_sales_price ELSE null END) tue_sales,
       SUM(CASE WHEN d_day_name='Wednesday' THEN ss_sales_price ELSE null END) wed_sales,
       SUM(CASE WHEN d_day_name='Thursday' THEN ss_sales_price ELSE null END) thu_sales,
       SUM(CASE WHEN d_day_name='Friday' THEN ss_sales_price ELSE null END) fri_sales,
       SUM(CASE WHEN d_day_name='Saturday' THEN ss_sales_price ELSE null END) sat_sales
FROM date_dim, store_sales, store
WHERE d_date_sk = ss_sold_date_sk AND s_store_sk = ss_store_sk
  AND s_gmt_offset = -5 AND d_year = 2000
GROUP BY s_store_name, s_store_id
ORDER BY s_store_name, s_store_id, sun_sales LIMIT 100
"""

TPCDS_Q55 = """
SELECT i_brand_id brand_id, i_brand brand,
       SUM(ss_ext_sales_price) ext_price
FROM date_dim, store_sales, item
WHERE d_date_sk = ss_sold_date_sk AND ss_item_sk = i_item_sk
  AND i_manager_id = 28 AND d_moy = 11 AND d_year = 1999
GROUP BY i_brand_id, i_brand ORDER BY ext_price DESC, brand_id LIMIT 100
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

# String workloads — using Trino TPC-H unprefixed column names
STR_LIKE_PREFIX = "SELECT COUNT(*) FROM lineitem WHERE comment LIKE 'a%'"
STR_LIKE_CONTAINS = "SELECT COUNT(*) FROM lineitem WHERE comment LIKE '%regular%'"
STR_LIKE_SUFFIX = "SELECT COUNT(*) FROM lineitem WHERE comment LIKE '%ly'"
STR_UPPER = "SELECT COUNT(*) FROM (SELECT UPPER(comment) as u FROM lineitem) t WHERE LENGTH(u) > 0"
STR_LOWER = "SELECT COUNT(*) FROM (SELECT LOWER(comment) as l FROM lineitem) t WHERE LENGTH(l) > 0"
STR_SUBSTRING = "SELECT COUNT(*) FROM (SELECT SUBSTRING(comment, 1, 10) as s FROM lineitem) t WHERE LENGTH(s) > 0"
STR_REPLACE = "SELECT COUNT(*) FROM (SELECT REPLACE(comment, ' ', '_') as r FROM lineitem) t WHERE LENGTH(r) > 0"
STR_PIPELINE = """
SELECT UPPER(returnflag), COUNT(*), SUM(extendedprice)
FROM lineitem WHERE comment LIKE '%regular%'
GROUP BY UPPER(returnflag) ORDER BY UPPER(returnflag)
"""

CONC_Q1 = """
SELECT returnflag, linestatus, SUM(quantity), SUM(extendedprice),
       AVG(discount), COUNT(*)
FROM lineitem WHERE shipdate <= DATE '1998-09-02'
GROUP BY returnflag, linestatus ORDER BY returnflag, linestatus
"""

# ── Main ─────────────────────────────────────────────────────────────────

def main():
    results = {}

    print("=" * 70)
    print("  Trino Benchmark Suite — REST API (server-side timing)")
    print("=" * 70)
    print()

    # TPC-H
    print("━━━ TPC-H Benchmarks ━━━")
    for sf in ["tiny", "sf1"]:
        print(f"\n  Scale: {sf}")
        for name, sql in [("q1", TPCH_Q1), ("q3", TPCH_Q3), ("q6", TPCH_Q6)]:
            key = f"tpch_{name}_{sf}"
            median, _ = run_benchmark(key, "tpch", sf, sql)
            results[key] = median

    # TPC-DS
    print("\n━━━ TPC-DS Benchmarks ━━━")
    for sf in ["sf1"]:
        print(f"\n  Scale: {sf}")
        for name, sql in [
            ("q3", TPCDS_Q3), ("q7", TPCDS_Q7), ("q19", TPCDS_Q19),
            ("q27", TPCDS_Q27), ("q43", TPCDS_Q43), ("q55", TPCDS_Q55),
            ("q96", TPCDS_Q96),
        ]:
            key = f"tpcds_{name}_{sf}"
            median, _ = run_benchmark(key, "tpcds", sf, sql)
            results[key] = median

    # String workloads
    print("\n━━━ String Workloads (TPC-H SF1) ━━━")
    for name, sql in [
        ("like_prefix", STR_LIKE_PREFIX), ("like_contains", STR_LIKE_CONTAINS),
        ("like_suffix", STR_LIKE_SUFFIX), ("upper", STR_UPPER),
        ("lower", STR_LOWER), ("substring", STR_SUBSTRING),
        ("replace", STR_REPLACE), ("pipeline", STR_PIPELINE),
    ]:
        key = f"string_{name}_sf1"
        median, _ = run_benchmark(key, "tpch", "sf1", sql)
        results[key] = median

    # Concurrency
    print("\n━━━ Concurrency (TPC-H Q1 SF1) ━━━")
    for npar in [1, 2, 4, 8]:
        print(f"  Concurrent queries: {npar}")
        # Warmup
        submit_query("tpch", "sf1", CONC_Q1)

        start = time.time()
        with ThreadPoolExecutor(max_workers=npar) as pool:
            futures = [pool.submit(submit_query, "tpch", "sf1", CONC_Q1) for _ in range(npar)]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    print(f"    Error: {e}")
        elapsed = (time.time() - start) * 1000
        print(f"    {npar} queries wall-clock: {elapsed:.2f}ms")
        results[f"concurrent_{npar}par"] = elapsed

    # Summary
    print("\n" + "=" * 70)
    print("  RESULTS SUMMARY (server-side ms)")
    print("=" * 70)
    for k, v in results.items():
        print(f"  {k:40s} {v:>10.2f} ms")

    # Save to file
    with open("/Users/ec2-user/code/prism/trino-bench/results-rest.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to trino-bench/results-rest.json")


if __name__ == "__main__":
    main()
