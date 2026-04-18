# Join Query Bottleneck & Distributed Aggregation Spec

## Summary

At TPC-H SF100, Prism beats Databricks on 8 of 9 benchmark queries. The lone holdout is the join-aggregation query:

```sql
SELECT o_orderstatus,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       COUNT(*) AS cnt
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
GROUP BY o_orderstatus
```

| System | Time |
|---|---|
| Databricks | 13.69s |
| Prism (today) | 178s+ |

Prism's local join is already fast (~19s per worker, 4-way parallel). The bottleneck is that each worker returns 37.5M joined rows to the Trino coordinator, which then aggregates 150M rows in a single JVM heap — that final aggregation takes 100+ seconds.

This doc specs the path to closing the gap: **distributed aggregation across Prism workers with inter-worker communication, bypassing the Trino coordinator for the final merge.**

## Why this is not a Trino patch

Trino's `PushAggregationIntoTableScan` rule requires the aggregation to sit directly on a `TableScan` with `step=SINGLE`. For the join-agg query:

1. `PushJoinIntoTableScan` converts `Join(lineitem, orders)` → `Project(rename) → TableScan[joined]`
2. The rule then needs `Aggregate → TableScan` but the plan is `Aggregate → Project → TableScan`
3. The Project doesn't collapse during the iterative optimizer phase: our connector's `applyProjection` returns empty for rename-only projections, and `RemoveRedundantIdentityProjections` only removes pure identity projects (`sym x → sym x`), not renames (`sym x → sym y`)
4. Patching the `step=SINGLE` check to accept `PARTIAL` — attempted and verified on 2026-04-18 — does not fix this. All four of the rule's `.matching()` predicates pass; only `source().matching(tableScan())` fails. See `/Users/ec2-user/.claude/projects/-Users-ec2-user-code-prism/memory/project_join_bottleneck.md` for details.

Additionally, pushing `PARTIAL` into a connector that returns fully-aggregated results (which Prism does) has correctness risk for non-decomposable aggregates (AVG, DISTINCT COUNT, stddev) — FINAL-of-partials is only correct for SUM/COUNT by coincidence.

**Conclusion:** the fix belongs in Prism's own execution layer, not in Trino core.

## Target architecture

### Today's pipeline

```
Trino coordinator ──push Join──▶ 4 Prism workers
                                    │
                                    │ local join (~19s each, parallel)
                                    ▼
                                 37.5M rows × 4 workers
                                    │
                                    ▼ (150M rows cross back to Trino)
Trino coordinator ──GROUP BY──▶ 100s+ in single JVM  ← bottleneck
```

### Target pipeline

```
Trino coordinator ──push (Join + GROUP BY)──▶ 4 Prism workers
                                                │
                                                │ local join + partial agg
                                                ▼
                                          ~3 rows × 4 workers
                                                │
                                                │ inter-worker transfer
                                                ▼
                                          elected reducer owns merge
                                                │ final agg on 12 rows
                                                ▼
                                          3 rows (one per o_orderstatus)
Trino coordinator ◀──trivial merge──────────────
```

The coordinator sees 3 rows instead of 150M. No JVM aggregation bottleneck.

## Design: Reducer Variant (cheaper first step)

### Data flow

Each query elects one worker as the reducer. Producers send their partial-aggregation output to the reducer over Arrow Flight. The reducer runs the final aggregation and returns the result to Trino.

### Moving parts

**Connector side (`PrismMetadata.java`):**
- Teach `applyAggregation` to fire when source is `Project(TableScan)` where Project is rename-only. Walk the Project's assignments and inline the renames before emitting the combined plan.
- Emit a new plan shape: `Reduce → PartialAgg → Join → Scans`.
- Reducer election: `reducer_index = hash(query_id) mod num_live_workers`. Trino already health-checks workers and only dispatches to the live set. Each query independently picks its reducer, spreading load over time.
- Pass peer list as part of each execute command (no discovery protocol needed).

**Substrait layer (`prism-substrait`):**
- New `PlanNode::Reduce` variant, or extend existing `Aggregate` with a "reducer mode" discriminator.
- Plan node carries: peer addresses, this worker's role (producer or reducer), query ID.

**Executor (`prism-executor`):**
- Reuse `hash_aggregate` for both phases — partial (on joined rows) and final (on 12 rows from peers).
- Producer path: run local join + partial agg → serialize Arrow batches → send to reducer via Flight → return empty result to Trino.
- Reducer path: run local join + partial agg → wait for N-1 peer batches → combine all → final agg → return to Trino.

**Transport:**
- Reuse the existing Arrow Flight server on each worker. Add a new action type `PartialAggBatch` carrying the query ID and serialized batch.
- No new ports, no new protocol. Just a new action on the existing endpoint.

### Resilience model

Prism today already fails a query if any worker dies mid-flight — there is no mid-query recovery, just Trino-level query retry. The reducer variant doesn't worsen this; it identifies one worker whose role matters more for a given query.

| Scenario | Behavior |
|---|---|
| Reducer dies **before** query starts | Trino health check excludes it; another worker elected transparently |
| Reducer dies **mid-query** | Partial aggs on it are lost. Query fails → Trino retries → new reducer from current live set |
| Producer dies mid-query | That shard's data is lost. Same as today: query fails → Trino retries |
| Peer link flap (producer → reducer) | Send fails. Option: fail query, or add a short in-band retry in the producer (~30 LOC) |

**We do not re-elect a reducer within a single running query.** To do so requires either (a) producers checkpoint their partial aggs so a new reducer can re-collect them, or (b) re-run the producers from scratch. Option (a) adds state machines and persistence; option (b) is the same cost as just failing the query. Query-level retry is the industry-standard answer for sub-minute analytical queries (Spark, Presto, Databricks all do this).

### Scale limits

The reducer variant works when the aggregation result set is small — the reducer sees at most `num_workers × num_groups` rows. For `GROUP BY o_orderstatus` that's 12 rows. For `GROUP BY customer_id` over millions of customers, the reducer becomes a memory bottleneck again. At that scale, graduate to the full shuffle mesh (below).

Migration path is clean: same `applyAggregation` surface on the connector side, same `PlanNode::Reduce` interface on the executor side. Only the transport layer changes.

## Design: Full Shuffle Mesh (later)

Each worker hash-partitions its partial-aggregation output on the GROUP BY key, sends foreign partitions to the right peers, and receives its own partition from all peers. Every worker does final aggregation on its slice of the groups in parallel.

```
Worker 0 ─── partial agg ───┐
Worker 1 ─── partial agg ──┐│
Worker 2 ─── partial agg ─┐││
Worker 3 ─── partial agg ┐│││
                         ▼▼▼▼
                  hash-shuffle on group key
                         ▼▼▼▼
                  each worker owns a slice
                         │
                  ▼ ▼ ▼ ▼ (final agg in parallel)
                  3 rows total, union back to Trino
```

This scales to any group cardinality. Used by Spark, Presto, Databricks, and Impala. Real work: inter-worker Flight endpoints as both server (for own partition) and client (to peers for foreign partitions), flow control, backpressure, graceful handling of peer failure during shuffle.

## Required prerequisite: Fused join + partial-agg

Even with the coordinator bottleneck removed, each worker's local join takes ~19s and produces 37.5M rows before partial aggregation starts. To actually beat Databricks' 13.69s, the join output should not be materialized — stream the probe results directly into the partial aggregation and discard rows that don't contribute to new groups.

**Implementation path:** extend `hash_join::hash_join_probe_chunked` to take an optional "inline aggregator" callback. When the caller is a fused join+agg, pass a `HashAggregate` state; each probe batch updates the aggregator and drops its rows. Expected result: per-worker time drops from ~19s to ~5–7s. Combined with the reducer variant, total query time should land in the 6–10s range — comfortably beating Databricks.

## Phased plan & effort

### Phase 1 — Connector fix + minimum-viable reducer (3–4 days)

Deliverables:
- `PrismMetadata.applyAggregation` looks through `Project(TableScan)` when rename-only.
- `PlanNode::Reduce` variant in `prism-substrait` and Rust executor wiring.
- Peer list passed in each execute command (no discovery protocol).
- Reducer election via `hash(query_id) mod num_workers`.
- Arrow Flight `PartialAggBatch` action on existing endpoint.

Unit: ~200 LOC Java + ~400 LOC Rust. Passes join-agg query end-to-end on 4-worker EC2 SF100.

Expected SF100 result: ~20–22s (vs today's 178s, vs Databricks' 13.69s). Coordinator bottleneck eliminated; local join still dominates.

### Phase 2 — Hardening (2–3 days)

Deliverables:
- Error propagation (peer send failure → fail query cleanly, Trino retries).
- Timeouts and backpressure bounds.
- Tracing / metrics for partial-agg batch sizes, peer send latencies, reducer wait time.
- Correctness sweep against all 9 benchmark queries (no regressions).

### Phase 3 — Fused join + partial-agg (3–5 days)

Deliverables:
- `hash_join::hash_join_probe_chunked` accepts optional inline aggregator.
- New executor path: `FusedJoinPartialAgg` node that fuses join probe with `hash_aggregate` state updates.
- Plan-builder change to emit `FusedJoinPartialAgg` when the connector pushes `Aggregate → Join`.

Expected SF100 result: 6–10s total — beats Databricks.

### Phase 4 — Benchmark + deploy (1 day)

- SF100 numbers, update PDF report, push to main.
- Update `docs/production-roadmap.md` to reconcile the "Arrow Flight shuffle — Done" line (today it refers to result transport only; after Phase 1 it covers inter-worker partial-agg transport; after full mesh, true shuffle).

**Total: 9–13 focused days.** Risk-adjusted: ~3 weeks. The single biggest unknown is Arrow Flight peculiarities under concurrent producer/reducer roles on the same server.

## De-risking shortcut

Before committing to Phase 1, spend **1–2 days on the connector fix alone** — make `applyAggregation` fire for the join-agg query by looking through `Project(TableScan)`, but keep the existing single-worker execution path. Measure: how much of the 178s is coordinator aggregation vs. local join vs. data transfer?

- If coordinator aggregation is really the 100s+ we think, the reducer variant design is confirmed.
- If something else dominates (e.g. data transfer is slower than expected, or local join is worse than measured), reshape the design before committing two more weeks.

This 1–2 day investment either validates the 3-week plan or redirects it — before any of the substantial work starts.

## Open questions

- **Cluster membership:** today, only Trino knows the Prism worker set (via connector config). The reducer variant needs Trino to pass the peer list in each execute command. Decide: static config per query, or pulled dynamically from a service registry? (Static is fine for Phase 1.)
- **Reducer rotation with stateful workers:** if/when Prism workers maintain local caches (Parquet block cache, join hash-tables), reducer rotation per query may cause cache misses on the reducer. Not a concern in Phase 1 (stateless execution), but worth tracking.
- **AVG and decomposable-aggregate semantics:** Prism's `applyAggregation` already decomposes AVG into SUM/COUNT for distributed correctness. Verify this still holds end-to-end through the reducer path. Unit test required in Phase 2.
