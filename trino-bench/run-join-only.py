#!/usr/bin/env python3
"""Run only the prism join query — for debugging."""

import json, re, sys, time, urllib.request, urllib.error

SERVER = "http://localhost:8080"

PRISM_JOIN = """
SELECT o_orderstatus,
       SUM(l_extendedprice * (1 - l_discount)) as revenue,
       COUNT(*) as cnt
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
GROUP BY o_orderstatus
ORDER BY revenue DESC
"""


def submit_query(catalog, schema, sql):
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
    rows = []

    while next_uri:
        time.sleep(0.05)
        req = urllib.request.Request(next_uri, headers={"X-Trino-User": "benchmark"})
        try:
            with urllib.request.urlopen(req) as resp:
                body = json.loads(resp.read())
            next_uri = body.get("nextUri", "")
            if "data" in body:
                rows.extend(body["data"])
        except urllib.error.HTTPError as e:
            print(f"HTTPError: {e}")
            break

    if "error" in body:
        err = body["error"]
        print(f"ERROR: {err.get('message', 'unknown')}")
        return None

    time.sleep(0.3)
    req = urllib.request.Request(
        f"{SERVER}/v1/query/{query_id}",
        headers={"X-Trino-User": "benchmark"},
    )
    with urllib.request.urlopen(req) as resp:
        stats = json.loads(resp.read())

    state = stats.get("state", "")
    if state == "FAILED":
        fi = stats.get("failureInfo", {})
        print(f"FAILED: {fi.get('message', 'unknown')}")
        return None

    qs = stats.get("queryStats", {})
    elapsed_str = qs.get("elapsedTime", "0ms")
    print(f"State: {state}, Elapsed: {elapsed_str}, Rows: {len(rows)}")
    for r in rows:
        print(f"  {r}")
    return rows


if __name__ == "__main__":
    print("Running prism join query...")
    submit_query("prism", "tpch", PRISM_JOIN)
