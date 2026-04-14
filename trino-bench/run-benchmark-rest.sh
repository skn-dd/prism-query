#!/usr/bin/env bash
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Trino Benchmark Suite — REST API (no CLI JVM overhead)
# Measures server-side execution time via Trino's query stats
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
set -uo pipefail

SERVER="http://localhost:8080"
RESULTS_DIR="/Users/ec2-user/code/prism/trino-bench/results-rest"
WARMUP_RUNS=1
MEASURE_RUNS=4

mkdir -p "$RESULTS_DIR"

# ── Submit query via REST and get server-side execution time ─────────────

run_query() {
    local name="$1"
    local catalog="$2"
    local schema="$3"
    local sql="$4"
    local outfile="$RESULTS_DIR/${name}.txt"

    echo "  Running: $name" >&2

    # Warmup
    for i in $(seq 1 $WARMUP_RUNS); do
        submit_and_wait "$catalog" "$schema" "$sql" >/dev/null 2>/dev/null || true
    done

    # Measured runs
    local times=()
    for i in $(seq 1 $MEASURE_RUNS); do
        local ms
        ms=$(submit_and_wait "$catalog" "$schema" "$sql" 2>/dev/null)
        times+=("$ms")
        echo "    Run $i: ${ms}ms" >&2
    done

    # Calculate median
    local sorted=($(printf '%s\n' "${times[@]}" | sort -n))
    local mid=$(( ${#sorted[@]} / 2 ))
    local median="${sorted[$mid]}"

    echo "$name $median ${times[*]}" >> "$outfile"
    echo "    Median: ${median}ms" >&2
    echo "$median"
}

submit_and_wait() {
    local catalog="$1"
    local schema="$2"
    local sql="$3"

    # Submit query
    local resp
    resp=$(curl -s -X POST "$SERVER/v1/statement" \
        -H "X-Trino-User: benchmark" \
        -H "X-Trino-Catalog: $catalog" \
        -H "X-Trino-Schema: $schema" \
        -d "$sql")

    local next_uri
    next_uri=$(echo "$resp" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('nextUri',''))" 2>/dev/null)

    if [ -z "$next_uri" ]; then
        echo "ERROR: no nextUri" >&2
        echo "0"
        return 1
    fi

    # Poll until complete
    local query_id
    query_id=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)

    while [ -n "$next_uri" ]; do
        sleep 0.1
        resp=$(curl -s "$next_uri" -H "X-Trino-User: benchmark")
        next_uri=$(echo "$resp" | python3 -c "
import sys,json
d=json.load(sys.stdin)
u=d.get('nextUri','')
if u: print(u)
" 2>/dev/null)
    done

    # Get query stats
    local stats
    stats=$(curl -s "$SERVER/v1/query/$query_id" -H "X-Trino-User: benchmark" 2>/dev/null)

    # Extract server-side execution time (elapsedTime in ms)
    local elapsed
    elapsed=$(echo "$stats" | python3 -c "
import sys, json, re
d = json.load(sys.stdin)
qs = d.get('queryStats', {})
# elapsedTime is like '1.23s' or '456ms'
et = qs.get('elapsedTime', '0ms')
# Parse duration string
m = re.match(r'([0-9.]+)(ms|s|m|h)', et)
if m:
    val, unit = float(m.group(1)), m.group(2)
    if unit == 's': val *= 1000
    elif unit == 'm': val *= 60000
    elif unit == 'h': val *= 3600000
    print(f'{val:.2f}')
else:
    print('0.00')
" 2>/dev/null)

    if [ -z "$elapsed" ]; then
        elapsed="0.00"
    fi
    echo "$elapsed"
}

# ── Header ───────────────────────────────────────────────────────────────

echo "╔══════════════════════════════════════════════════════════════════════╗" >&2
echo "║     Trino Benchmark Suite — REST API (server-side timing)           ║" >&2
echo "╚══════════════════════════════════════════════════════════════════════╝" >&2
echo "" >&2

# Verify cluster
NODE_COUNT=$(curl -s "$SERVER/v1/statement" -H "X-Trino-User: benchmark" \
    -d "SELECT count(*) FROM system.runtime.nodes WHERE state='active'" | \
    python3 -c "import sys,json; print(json.load(sys.stdin).get('id','unknown'))" 2>/dev/null)
echo "Cluster query ID: $NODE_COUNT" >&2
echo "Warmup runs: $WARMUP_RUNS, Measured runs: $MEASURE_RUNS" >&2
echo "" >&2

# ── TPC-H ────────────────────────────────────────────────────────────────

echo "━━━ TPC-H Benchmarks ━━━" >&2

for sf in "tiny" "sf1"; do
    echo "" >&2
    echo "  Scale: $sf" >&2

    run_query "tpch_q1_${sf}" "tpch" "$sf" "
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity) as sum_qty,
       SUM(l_extendedprice) as sum_base_price,
       SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
       AVG(l_quantity) as avg_qty,
       AVG(l_extendedprice) as avg_price,
       AVG(l_discount) as avg_disc,
       COUNT(*) as count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus"

    run_query "tpch_q3_${sf}" "tpch" "$sf" "
SELECT l_orderkey,
       SUM(l_extendedprice * (1 - l_discount)) as revenue,
       o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'
  AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10"

    run_query "tpch_q6_${sf}" "tpch" "$sf" "
SELECT SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24"
done

# ── TPC-DS ───────────────────────────────────────────────────────────────

echo "" >&2
echo "━━━ TPC-DS Benchmarks ━━━" >&2

for sf in "sf1"; do
    echo "" >&2
    echo "  Scale: $sf" >&2

    run_query "tpcds_q3_${sf}" "tpcds" "$sf" "
SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,
       SUM(ss_ext_sales_price) sum_agg
FROM date_dim dt, store_sales, item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
  AND store_sales.ss_item_sk = item.i_item_sk
  AND item.i_manufact_id = 128
  AND dt.d_moy = 11
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year, sum_agg DESC, brand_id
LIMIT 100"

    run_query "tpcds_q7_${sf}" "tpcds" "$sf" "
SELECT i_item_id,
       AVG(ss_quantity) agg1, AVG(ss_list_price) agg2,
       AVG(ss_coupon_amt) agg3, AVG(ss_sales_price) agg4
FROM store_sales, customer_demographics, date_dim, item, promotion
WHERE ss_sold_date_sk = d_date_sk AND ss_item_sk = i_item_sk
  AND ss_cdemo_sk = cd_demo_sk AND ss_promo_sk = p_promo_sk
  AND cd_gender = 'M' AND cd_marital_status = 'S'
  AND cd_education_status = 'College'
  AND (p_channel_email = 'N' OR p_channel_event = 'N')
  AND d_year = 2000
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100"

    run_query "tpcds_q19_${sf}" "tpcds" "$sf" "
SELECT i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
       SUM(ss_ext_sales_price) ext_price
FROM date_dim, store_sales, item, customer, customer_address, store
WHERE d_date_sk = ss_sold_date_sk AND ss_item_sk = i_item_sk
  AND i_manager_id = 8 AND d_moy = 11 AND d_year = 1998
  AND ss_customer_sk = c_customer_sk AND c_current_addr_sk = ca_address_sk
  AND SUBSTRING(ca_zip, 1, 5) <> SUBSTRING(s_zip, 1, 5)
  AND ss_store_sk = s_store_sk
GROUP BY i_brand_id, i_brand, i_manufact_id, i_manufact
ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact
LIMIT 100"

    run_query "tpcds_q27_${sf}" "tpcds" "$sf" "
SELECT i_item_id, s_state,
       GROUPING(s_state) g_state,
       AVG(ss_quantity) agg1, AVG(ss_list_price) agg2,
       AVG(ss_coupon_amt) agg3, AVG(ss_sales_price) agg4
FROM store_sales, customer_demographics, date_dim, store, item
WHERE ss_sold_date_sk = d_date_sk AND ss_item_sk = i_item_sk
  AND ss_store_sk = s_store_sk AND ss_cdemo_sk = cd_demo_sk
  AND cd_gender = 'M' AND cd_marital_status = 'S'
  AND cd_education_status = 'College' AND d_year = 2002
  AND s_state IN ('TN')
GROUP BY ROLLUP(i_item_id, s_state)
ORDER BY i_item_id, s_state
LIMIT 100"

    run_query "tpcds_q43_${sf}" "tpcds" "$sf" "
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
ORDER BY s_store_name, s_store_id, sun_sales
LIMIT 100"

    run_query "tpcds_q55_${sf}" "tpcds" "$sf" "
SELECT i_brand_id brand_id, i_brand brand,
       SUM(ss_ext_sales_price) ext_price
FROM date_dim, store_sales, item
WHERE d_date_sk = ss_sold_date_sk AND ss_item_sk = i_item_sk
  AND i_manager_id = 28 AND d_moy = 11 AND d_year = 1999
GROUP BY i_brand_id, i_brand
ORDER BY ext_price DESC, brand_id
LIMIT 100"

    run_query "tpcds_q96_${sf}" "tpcds" "$sf" "
SELECT COUNT(*)
FROM store_sales, household_demographics, time_dim, store
WHERE ss_sold_time_sk = time_dim.t_time_sk
  AND ss_hdemo_sk = household_demographics.hd_demo_sk
  AND ss_store_sk = s_store_sk
  AND time_dim.t_hour = 20 AND time_dim.t_minute >= 30
  AND household_demographics.hd_dep_count = 7
  AND store.s_store_name = 'ese'
ORDER BY COUNT(*) LIMIT 100"
done

# ── String Workloads ─────────────────────────────────────────────────────

echo "" >&2
echo "━━━ String Workload Benchmarks (TPC-H SF1) ━━━" >&2

run_query "string_like_prefix_sf1" "tpch" "sf1" "
SELECT COUNT(*) FROM lineitem WHERE l_comment LIKE 'a%'"

run_query "string_like_contains_sf1" "tpch" "sf1" "
SELECT COUNT(*) FROM lineitem WHERE l_comment LIKE '%regular%'"

run_query "string_like_suffix_sf1" "tpch" "sf1" "
SELECT COUNT(*) FROM lineitem WHERE l_comment LIKE '%ly'"

run_query "string_upper_sf1" "tpch" "sf1" "
SELECT COUNT(*) FROM (SELECT UPPER(l_comment) as u FROM lineitem) t WHERE LENGTH(u) > 0"

run_query "string_lower_sf1" "tpch" "sf1" "
SELECT COUNT(*) FROM (SELECT LOWER(l_comment) as l FROM lineitem) t WHERE LENGTH(l) > 0"

run_query "string_substring_sf1" "tpch" "sf1" "
SELECT COUNT(*) FROM (SELECT SUBSTRING(l_comment, 1, 10) as s FROM lineitem) t WHERE LENGTH(s) > 0"

run_query "string_replace_sf1" "tpch" "sf1" "
SELECT COUNT(*) FROM (SELECT REPLACE(l_comment, ' ', '_') as r FROM lineitem) t WHERE LENGTH(r) > 0"

run_query "string_pipeline_sf1" "tpch" "sf1" "
SELECT UPPER(l_returnflag), COUNT(*), SUM(l_extendedprice)
FROM lineitem WHERE l_comment LIKE '%regular%'
GROUP BY UPPER(l_returnflag)
ORDER BY UPPER(l_returnflag)"

# ── Concurrency ──────────────────────────────────────────────────────────

echo "" >&2
echo "━━━ Concurrency Benchmarks (TPC-H Q1 SF1) ━━━" >&2

CONC_SQL="SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice), AVG(l_discount), COUNT(*) FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"

for npar in 1 2 4 8; do
    echo "  Concurrent queries: $npar" >&2

    # Warmup
    submit_and_wait "tpch" "sf1" "$CONC_SQL" >/dev/null 2>/dev/null || true

    start_ns=$(python3 -c "import time; print(int(time.time_ns()))")

    pids=()
    for i in $(seq 1 $npar); do
        submit_and_wait "tpch" "sf1" "$CONC_SQL" >/dev/null 2>/dev/null &
        pids+=($!)
    done

    for pid in "${pids[@]}"; do
        wait $pid 2>/dev/null || true
    done

    end_ns=$(python3 -c "import time; print(int(time.time_ns()))")
    elapsed_ms=$(python3 -c "print(f'{($end_ns - $start_ns) / 1_000_000:.2f}')")

    echo "    $npar queries wall-clock: ${elapsed_ms}ms" >&2
    echo "concurrent_q1_${npar}par $elapsed_ms" >> "$RESULTS_DIR/concurrency.txt"
done

# ── Summary ──────────────────────────────────────────────────────────────

echo "" >&2
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" >&2
echo "All benchmarks complete. Results in $RESULTS_DIR/" >&2
echo "" >&2

echo "╔══════════════════════════════════════════════════════════════════════╗" >&2
echo "║              RESULTS SUMMARY (server-side ms)                       ║" >&2
echo "╚══════════════════════════════════════════════════════════════════════╝" >&2

for f in "$RESULTS_DIR"/*.txt; do
    [ -f "$f" ] && while read -r name median rest; do
        printf "  %-35s %s ms\n" "$name" "$median" >&2
    done < "$f"
done
