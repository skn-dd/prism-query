# Join Query Fix: Wiring the Distributed Path Through Real SQL

## Summary

At TPC-H SF100, the join-aggregation query wedges the Trino coordinator at 800%+ CPU for 170s+ because each worker returns 37.5M joined rows and the coordinator aggregates 150M rows single-threaded.

Commit `01126ee` landed a distributed-aggregation infrastructure ("reducer variant": producers partial-aggregate, one elected reducer merges via peer Flight transport). The dispatch in `PrismPageSource.getNextPage` already selects this path:

```java
// PrismPageSource.java:119-120
if (workerCount > 1 && hasAggregation() && hasJoin()) {
    executeOnReducerWorker();
}
```

**The problem:** on the real SQL join-agg query, `hasAggregation()` returns false because the pushed plan contains `Join(Project(Scan), Project(Scan))` with no `Aggregate` node on top. `applyAggregation` is never called after `applyJoin` succeeds — Trino's `PushAggregationIntoTableScan` rule is structurally blocked (see `docs/join-query-bottleneck.md` and `project_join_bottleneck.md`). The `BenchmarkCoordinator` code path works because it hand-builds a pushed plan with the Aggregate pre-attached, bypassing the SPI entirely. No real SQL query ever enters the distributed path.

## Root cause

Trino's rule chain for the query

```sql
SELECT o_orderstatus, SUM(...), COUNT(*)
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
GROUP BY o_orderstatus
```

1. `PushJoinIntoTableScan` → `Project(rename) → TableScan[joined]` ✅ our `applyJoin` absorbs it
2. `PushAggregationIntoTableScan` → requires `Aggregate → TableScan` with `step=SINGLE`
3. Plan at that point is `Aggregate → Project(rename) → TableScan`
4. Two failures stack:
   - `Project(rename)` does not collapse (verified 2026-04-18)
   - Rule predicate `step=SINGLE` doesn't match when Trino splits into `PARTIAL/FINAL` for distributed grouping

Commit `01126ee` tried to fix (a) by making `applyProjection` absorb rename-only projections. That works in isolation but causes the 4-query regression fixed in `PrismMetadata.java:425-439` (collision between original-table `columnIndex` and post-Aggregate output indices when `groupingColumnMapping` substitutes handles). My fix skips the rename-absorption over `Aggregate`, preserving the intent for the Join case.

Even with rename-absorption working, (b) — `step=SINGLE` vs `PARTIAL` — still blocks `PushAggregationIntoTableScan` on the join-agg query. So `applyAggregation` never fires → `hasAggregation()` stays false → distributed path never selected.

## The fix

Stop depending on Trino to compose the pushed plan. Compose it ourselves inside `applyJoin` when a downstream aggregate is visible, or detect the pattern in `PrismPageSource` and rewrite the plan before dispatch.

### Option A — Fused join+agg in `applyJoin` (preferred)

After `applyJoin` succeeds, peek at the caller chain in Trino's context and see if the immediate parent is an Aggregate node. If yes, synthesize `Aggregate(Join(...))` as the pushed plan in one shot, returning the aggregate's output columns as the `ConnectorTableHandle`'s columns. This is what `BenchmarkCoordinator` does manually today.

**Where:** `PrismMetadata.applyJoin` at `trino/prism-bridge/src/main/java/io/prism/plugin/PrismMetadata.java` (extend existing handler).

**Mechanism:** Trino's `applyJoin` SPI receives `ConnectorSession`, join type, left/right handles, clauses, and assignments — but *not* the containing aggregate. Two sub-options:

- **A1 — Speculative push.** When `applyJoin` is called, always produce `Project(Join(...))`. Then when `applyAggregation` is later called on that handle, convert the full `Aggregate(Project(Join))` into `Aggregate(Join)` by absorbing the rename-only Project. This is what `01126ee` intended. It requires `applyAggregation` to fire, which it won't because of (b).

- **A2 — Planner hint via session property.** Add `prism.force_distributed_join_agg` session property. When set, `applyJoin` emits a *sentinel* plan — `Join(...)` with a side channel saying "expect aggregate, materialize as `Aggregate(Join)` before execution." `PrismPageSource.getNextPage` checks the sentinel, walks the Trino plan fragment it receives (available through `ConnectorSplit.getProperties()` or `ConnectorTableHandle` metadata we fill in), and rebuilds the pushed plan. Crude but avoids fighting the optimizer.

### Option B — Rewrite in PrismPageSource before dispatch (simplest)

`PrismPageSource` already has access to `tableHandle.getPushedPlan()`. When the plan is `Project(Join)` or `Join` and Trino is about to add a group-by on top (detectable via the `ConnectorPageSinkProvider` or the session's query statement), rewrite the plan locally to `Aggregate(Join)` using the grouping and aggregate information reconstructed from the Trino plan fragment metadata available to the connector.

**Problem:** the page-source doesn't receive the containing Aggregate node; it only sees its own sub-plan. This option requires Trino to expose the outer plan node, which it doesn't through the page-source SPI.

### Option C — Patch Trino core (rejected)

Patching `PushAggregationIntoTableScan.matching()` to accept `step=PARTIAL` plus relaxing the `TableScan` match to accept `Project → TableScan` is an 80-line Trino patch. Rejected because:
- fork maintenance cost
- FINAL-of-PARTIAL correctness risk for non-decomposable aggregates (AVG, DISTINCT COUNT) when the connector returns fully-aggregated results
- memory note `project_metadata_delegation_blocker.md` already flagged upstream SPI gaps as pending Trino PRs

### Option D — Interceptor in our plugin's `ConnectorPlanOptimizer` (preferred long-term)

Trino 468 exposes `ConnectorPlanOptimizerProvider` via the SPI. A connector can register a `ConnectorPlanOptimizer` that runs AFTER the standard rules. Our optimizer walks the plan subtree rooted at our `TableScan`, sees `Aggregate → Project(rename) → TableScan(pushedPlan=Join(...))`, and rewrites the `TableScan`'s handle to contain `Aggregate(Join(...))`, stripping the upstream `Project` and `Aggregate` nodes.

**Where:** new class `PrismPlanOptimizer` in `trino/prism-bridge/src/main/java/io/prism/plugin/`, registered via `PrismConnector.getConnectorPlanOptimizerProvider()`.

**Why this works:** `ConnectorPlanOptimizer` runs as part of the iterative optimizer but operates on subtrees the connector "owns." Unlike `PushAggregationIntoTableScan`, it has no `step=SINGLE` precondition — we decide when to fire.

**Correctness:** our Rust executor already returns fully-aggregated results. The optimizer only fires when the Aggregate is decomposable at our connector boundary (SUM/COUNT/MIN/MAX) or already at `step=SINGLE`. For AVG, we decompose into SUM+COUNT and reconstruct at the reducer.

## Recommended plan

1. **Implement Option D** — `PrismPlanOptimizer` with `ConnectorPlanOptimizerProvider` registration. Match `Aggregate → Project(rename) → TableScan(Join)` and rewrite to `TableScan(Aggregate(Join))`. ~200 LOC.
2. **Keep rename-absorption fix in place** — `PrismMetadata.java:433` `!planIsAggregate` guard prevents the 4-query regression; the optimizer in (1) handles the Join rename-absorption that `01126ee` was trying to achieve.
3. **Verify distributed path fires** — the existing `hasAggregation() && hasJoin()` branch in `PrismPageSource.java:119` will now trigger because the pushed plan contains `Aggregate(Join(...))`.
4. **Benchmark** — expected improvement: join query from 170s+ to 10–15s (beat Databricks 13.69s).
5. **Phase 3 (fused join+partial-agg in the executor)** — `hash_join::hash_join_probe_chunked` currently takes no aggregator callback. Wire an optional per-chunk partial-agg so we never materialize 37.5M intermediate rows per worker. ~300 LOC in `native/prism-executor/src/hash_join.rs`. Expected: additional 2–4x on join query.

## Why not fix inside `applyProjection` alone

The rename-absorption in `applyProjection` only helps if `applyAggregation` fires afterward. On the join-agg query, the optimizer never calls `applyAggregation` after `applyJoin` because of the `step=SINGLE` precondition. Absorbing the rename in `applyProjection` is necessary but not sufficient.

## Testing

- `DistributedAggregateE2ETest` — existing; tests `BenchmarkCoordinator` path directly
- New: `PrismPlanOptimizerTest` — unit test the rewrite on a fake plan
- New: `JoinAggSqlE2ETest` — submit the actual SQL through `/v1/statement`, assert the distributed path fires (check worker logs for `executeOnReducerWorker`) and result is correct
- Re-run `bench9.py` at SF100; join must drop below 15s with no Trino wedge

## Open questions

- **Memory note `project_metadata_delegation_blocker.md`** says `MetadataProvider` has SPI gaps. Does `ConnectorPlanOptimizerProvider` have the access we need? Verify in Trino 468 source before committing.
- **Fault tolerance** with FTE enabled — `executeOnReducerWorker` assumes all workers are healthy. Reducer failure currently fails the query. Acceptable for the benchmark; revisit before production.
