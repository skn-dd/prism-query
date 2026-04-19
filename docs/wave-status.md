# Prism Wave Status

_Rolling log of delivery waves. Companion to `production-plan.md` (strategy, phase design) and `production-roadmap.md` (gap analysis)._

_Last updated: 2026-04-19._

## Wave 1 — complete (2026-04-18)

Landed on `main`:
- `2c861cc` — object-store abstraction + async Parquet reader (Phase 1.1). Uses `object_store = "0.12"` with `aws`/`gcp`/`azure` features. All existing tests plus 5 new green on EC2.
- `d21cd24` — worker TOML config (Phase 1.4). `WorkerConfig` loader with precedence: defaults → file → `PRISM_*` env → CLI flags.

Audit findings folded into Wave 2 work:
- `PrismMetadata.java` `TPCH_TABLES` is the single biggest parallel-universe — addressed by Phase 1.3.
- `prism-flight` is a parallel shuffle bypassing Trino's `ExchangeManager` SPI — addressed by new Phase 1.5.
- `PrismExecutionException` doesn't map to `TrinoException` — addressed by new Phase 1.6.
- OSI semantic layer (metric definitions) verified as genuinely distinct; KEEP.
- Ranger integration uses Trino's native SPI; KEEP.
- No parallel credential, audit, auth, or session-property mechanisms found.

## Wave 2a — Trino SPI correctness (landed)

| Commit | Slice |
|---|---|
| `2690133` | TrinoException + `StandardErrorCode` classification at every user-visible boundary (Phase 1.6). |
| `6b86810` | `PrismSessionProperties` registered via `Connector.getSessionProperties()` — `prism.acceleration_enabled`, `prism.memory_budget_gb`, `prism.prefer_single_worker`. |
| `f54e6e0` | `EventListener` integration — `acceleration_used`, `worker_ids`, `rust_runtime_ms` emitted via Trino event stream. `bytes_scanned` still 0 (needs Flight response metadata — tracked below). |
| `ecc2151` | mTLS on Arrow Flight — `PrismFlightExecutor.TlsOptions` via `prism.tls.*` properties; `PrismErrorMapper.forCode(CONFIGURATION_INVALID, ...)` for all config failures. Unblocks the D3 credential transport. |
| `74b7f2d` | Flight protocol v2 + Parquet schema-evolution via `arrow_cast::cast` — `tables: { name: { uris: [...], store_key?: "..." } }` on the wire. |

## Wave 2b — workspace + exchange SPI (landed)

| Commit | Slice |
|---|---|
| `e539aeb` | Arrow 57 / DataFusion 51 / parquet 57 / tonic 0.14 / prost 0.14 workspace bump. `has_min_max_set()` gate removed in parquet 57; `stats_might_match` tolerates absent stats. |
| `01126ee` / `a2ce209` | Distributed join aggregation path — new `prism-substrait/executor.rs`, 2-phase reducer, `BenchmarkCoordinator` hardening, 385-line `DistributedAggregateE2ETest`. Closes the SF100 join gap from `docs/join-query-bottleneck.md`. |
| `5b4b51f` | `trino/prism-exchange/` — `ExchangeManager` SPI plugin (Phase 1.5 core). `floorMod(partitionId, workerCount)` routing, storage key `exchange/{exchangeId}/{partitionId}`, `PrismFlightTlsOptions` mTLS mirror, 20/20 tests green. |

## Wave 2b — metadata delegation (blocked; no code shipped)

Investigation confirmed on 2026-04-19 that **Trino 468/480 public SPI cannot support delegation.** `ConnectorContext.getMetadataProvider().getRelationMetadata()` returns only `ConnectorTableSchema` — no `ConnectorTableHandle`, no `ConnectorSplitSource`, no file URIs, no Iceberg/Delta column IDs. `TableScanRedirectApplicationResult` flows the wrong direction. The only paths into another connector's state are `trino-main` internals (`CatalogManager`, `ConnectorServicesProvider`, `CatalogMetadata`) that the plugin classloader deliberately hides.

Per the long-term-correct preference, we did not ship a driver-side REST workaround or a `trino-main` reflection hack. `TPCH_TABLES` stays as the only table resolution path until the upstream Trino SPI extends `MetadataProvider` with read-only `getTableHandle` / `getSplitSource`. See `docs/join-query-bottleneck.md` for the investigation memo.

## Pending items (2026-04-19) — ranked by long-term-correctness pressure

**P0 — structural.**
1. **Upstream Trino SPI PR** — `MetadataProvider.getTableHandle` / `getSplitSource` read-only for acceleration-layer use. Unblocks items 2 and the column-ID threading in `prism-substrait`. 2–4 weeks PR cycle; not an agent job.
2. **Iceberg reader** (`native/prism-iceberg/`) — manifest parsing can start standalone; full scan pipeline depends on item 1 for file-URI delivery.
3. **Memory tracking + spill.** `getMemoryUsage()` returns 0 today; Trino backpressure is blind. `hash_join`/`hash_aggregate` build unbounded. First slice: honest `peak_memory_bytes` reporting through hash builds. Spill-to-disk is a second slice.

**P1 — correctness and completeness.**
4. **Decimal128 end-to-end** — f64 cast path loses precision for SUM of money. Needs `ScalarValue::Decimal128(i128, u8, i8)` threaded through filter/agg/consumer.
5. **`COUNT_DISTINCT`** — `Vec<u64>.contains` → `HashSet<u64>`. XS, correctness.
6. **Rust Flight actions `close_exchange` + `drop_exchange`** — required to flip `PrismExchangeSink.supportsConcurrentReadAndWrite=true` and to free worker state on exchange completion.
7. **`PrismExchangeSink.isBlocked`** — currently always `NOT_BLOCKED`. Wire Netty watermark so Trino sees real backpressure.
8. **ORC reader** — `orc-rust 0.7.1` unblocked by Arrow 57. Standalone; adds format dispatch in `native/prism-bench/src/handler.rs` + new `orc_scan.rs` in `prism-executor`.

**P2 — hygiene.**
9. **Remove `native/prism-flight/src/shuffle_{writer,reader}.rs`** — parallel-universe with `ExchangeManager` ownership. Only referenced from tests/bench.
10. **`prism-flight-client` shared Java module** — dedupe `PrismFlightTlsOptions` mirror between `prism-bridge` and `prism-exchange`. Defer until both consumers are stable.
11. **Substrait consumer refactor** — 4 deprecated enum variants: `CountMode::Count` → `CountAfterExpr`, `OffsetMode::Offset`, `Kind::Timestamp` → `PrecisionTimestamp`, `Grouping::grouping_expressions` → `expression_references`.
12. **`EventListener.bytes_scanned`** — needs a Flight response metadata field propagated from the worker.
13. **FTE (`setOutputSelector`)** — deferred; real work only when a customer asks.

**P3 — platform.**
14. **ExtraCredentials passthrough** — `ConnectorSession.getIdentity().getExtraCredentials()` → Rust worker `DoAction` payload. Requires mTLS (already in place).
15. **Phases 2–9** — enablement model, container hardening, Helm, service discovery, observability, security finalization, reliability, CI/CD. Tracked in the respective phase sections of `production-plan.md`.

## Next slice — parallelizable agent work

Four independent pieces that can land concurrently, each touching a disjoint path:

| Agent | Scope | Item | Size | Path |
|---|---|---|---|---|
| A | ORC reader + format dispatch | 8 | S | `native/prism-executor/src/orc_scan.rs`, `handler.rs` |
| B | `close_exchange` / `drop_exchange` Flight actions | 6 | S | Rust Flight handler, `PrismExchangeSource` flip |
| C | `COUNT_DISTINCT` → `HashSet<u64>` | 5 | XS | `native/prism-executor/src/hash_aggregate.rs` |
| D | Memory-tracking scaffold (reporting only, no spill) | 3 | M | `native/prism-executor/src/{memory,hash_join,hash_aggregate}.rs`, `PrismPageSource.java` |

**Author track (not an agent job):** open the upstream Trino SPI PR extending `MetadataProvider` (item 1). 2–4 week cycle.

**Second pass after the slice above:** Decimal128 (item 4), remove shuffle parallel-universe (item 9), factor `prism-flight-client` (item 10) once both plugins have stabilized.

## Still missing after the slice — not tracked in Phases 2–9

Items that remain after the full wave-status slice (agents A–D + second pass + author-track SPI PR) lands. These live outside the Phase 2–9 production-readiness scope in `production-plan.md` and need explicit follow-up here.

### Gated on metadata delegation (item 1)

These cascade from the Trino SPI gap. No code can ship until the upstream `MetadataProvider.getTableHandle` / `getSplitSource` PR lands.

- **Hive Metastore (HMS) as a delegated catalog source.** The named-catalog delegation pattern (`prism.delegate-catalog-name=hive`) is meant to cover HMS, Glue, and Iceberg REST uniformly via item 1's SPI PR. Phase 1.3 describes the pattern; the SPI gap means it produces no running code today. Without the PR, `TPCH_TABLES` remains the only table resolution path for all three metadata source types.
- **Ranger row-filter and column-mask cascade through `ConnectorMetadata` hooks** — only one part of a larger Ranger gap. See [Ranger row/column security — implementation plan](#ranger-rowcolumn-security--implementation-plan) below for the full breakdown; only gap 6 of that plan is gated on item 1.
- **Column-ID threading for schema evolution.** Iceberg/Delta `fieldId` values live in the delegated catalog's metadata; without delegation there's no source of IDs, so schema evolution through projected scans cannot round-trip correctly.

### SQL feature coverage (not in Phases 2–9)

Phases 2–9 cover operational readiness (enablement, containers, Helm, discovery, observability, security, reliability, CI/CD) — they do **not** cover language surface. These feature gaps show up in real workloads and block adoption on their own:

- **Window functions** — `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `LAG`, `LEAD`, `SUM/AVG/MIN/MAX OVER`. Heavy analytics use. Needs `PlanNode::Window` + Substrait `ConsistentPartitionWindowRel` consumer + frame evaluation.
- **Set operations** — `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT` via Substrait `SetRel`.
- **Cross join + non-equi / theta join** — nested-loop fallback when `extract_join_keys()` finds no equi-join condition.
- **Complex types** — `Timestamp(µs)`, `Interval`, `List`, `Struct`, `Map` threaded through filter/projection/aggregation. Today most of these fall back to error at the Substrait consumer.

### Explicitly deferred post-v1 (noted for completeness)

Called out in `production-plan.md` as deliberate non-goals; listed here so the gap list is exhaustive:

- Write path (`INSERT` / `MERGE` / `DELETE`).
- Hudi support.
- JDBC federation (roadmap Path B — reopen only with profiled evidence of JVM operator bottleneck).
- Full engine replacement (roadmap Path C).

## Ranger row/column security — implementation plan

### Current state (2026-04-19)

Trino's `SystemAccessControl` SPI is already wired cluster-wide via `docker/etc/ranger-access-control.properties` (`access-control.name=ranger`, `ranger.service-name=prism`). Coarse table/schema grants are enforced by the coordinator before the plan reaches `PrismMetadata`, so **table/schema-level authorization works today**. Row filters and column masks, however, have three independent gaps that all silently compromise them:

| Symptom today | Root cause |
|---|---|
| Policies using `current_user()` / `is_member_of_group()` produce wrong results | `ConnectorSession.getIdentity()` is not plumbed into the Rust worker — grep confirms zero `getIdentity` / `ExtraCredentials` references in `trino/prism-bridge/src/main/`. |
| Every non-trivial column mask errors or mis-evaluates | `native/prism-substrait/src/consumer.rs` has zero coverage for `IfThen` / `CASE` expressions — verified by grep. Every practical column mask is `CASE WHEN has_access() THEN col ELSE mask(col) END`. |
| Policy audit invisible downstream | `f54e6e0` EventListener does not emit which policies applied, which columns were masked, or whether a row filter was active. |

The common framing that "Prism inherits Ranger for free via Trino's SPI" is true only for table/schema-level grants. Row and column security require explicit implementation work.

### Gaps and fixes

**Gap 1 — Session identity propagation** (unblocked, foundational)
- Thread `session.getIdentity()` (user, groups, roles, `ExtraCredentials`) through `PrismFlightExecutor.executeQuery()` into the `DoAction` payload. Extend the protocol-v2 envelope (landed in `74b7f2d`) with a `session_context { user, groups[], roles[] }` field.
- Rust worker decodes `session_context` into a per-query evaluation context accessible by the expression engine.
- mTLS is already in place (`ecc2151`) — hard requirement since identity now travels on the wire.

**Gap 2 — Built-in ACL scalar functions in Rust** (unblocked; depends on gap 1)
- `current_user()`, `current_role()`, `current_catalog()`, `current_schema()`, `is_member_of(group)`, `current_groups()`.
- Sourced from the session context populated by gap 1. No side-channel to a Ranger server — the engine's already-resolved identity is the only input.
- Substrait consumer maps Trino's security function URIs to the Rust implementations.

**Gap 3 — `IfThen` / `CASE` in Substrait consumer** (unblocked; zero coverage today)
- Add `Expression::IfThen` consumer path in `native/prism-substrait/src/consumer.rs`.
- Evaluation via arrow-rs `case_when` kernel.
- Test coverage: three-way CASE with literal branches, CASE on identity-function output, nested CASE, CASE referring to other columns. Without this, column masking is fundamentally unimplementable — grep confirmed.

**Gap 4 — String-masking kernels** (unblocked)
- Audit current coverage in `native/prism-executor/src/filter_project.rs`.
- Fill in what Ranger policies typically use: `substring` / `substr`, `replace`, `regexp_replace`, `concat`, `length`, `lower`, `upper`, `lpad` / `rpad`.
- Policies commonly mask as `concat('***-**-', substring(ssn, -4))`, `regexp_replace(email, '(?<=.).(?=[^@]*@)', '*')`, or fully-literal replacement — the full set needs to route through arrow-rs string kernels.

**Gap 5 — UDF fallback strategy** (unblocked)
Ranger policies often embed custom Java UDFs via Trino function plugins. These cannot run in Rust. Three behaviors need explicit definition:
- **Static UDF (plan-time evaluable):** coordinator pre-evaluates, materializes the result as a Substrait literal before sending to Prism.
- **Per-row UDF:** coordinator inspects the `ViewExpression` function URIs at plan time. If any URI is not in Prism's supported set, either (a) fail with `TrinoException(NOT_SUPPORTED, "Ranger policy <id> uses UDF <x> not available in accelerated path")`, or (b) transparently route the query through standard Trino execution via an internal `prism.acceleration_enabled=false` override. Pick one; document it.
- **Registry:** maintain `PrismSupportedFunctions` — the union of built-in scalars, ACL functions (gap 2), string kernels (gap 4), and the generic numeric/date vocabulary already consumed. Consulted by the plan-time routing decision.

**Gap 6 — `ConnectorMetadata.getRowFilters` / `getColumnMasks` hooks** (gated on item 1)
Connector-level hooks matter for two reasons:
- **Pushdown optimization:** row filter into Parquet row-group stats, Iceberg partition pruning, predicate elision.
- **Delegated-catalog cascade:** when `prism.delegate-catalog-name=iceberg` is configured (item 1), Ranger policies attached to the iceberg catalog must flow through Prism's metadata to the Substrait plan.
- Forward to the delegated catalog's implementation once delegation exists. For the current `TPCH_TABLES` path: return empty lists — correctness is already handled by the engine-level plan rewrite; there is no pushdown benefit for synthetic TPCH data.

**Gap 7 — EventListener policy audit fields** (unblocked)
Extend the `f54e6e0` EventListener payload:
- `ranger_policies_applied: List<String>` — policy IDs evaluated.
- `ranger_masked_columns: List<CatalogSchemaTableColumnName>` — columns that had a mask applied.
- `ranger_row_filter_applied: bool` — whether any row filter attached.
- Source: inspect the rewritten plan reaching `PrismMetadata.getTableHandle()` — by that point the coordinator has already resolved and attached the policies.

**Gap 8 — Integration test surface** (unblocked; depends on gaps 1–5 and 7)
Policy fixtures in `docker/etc/ranger/` (directory exists but is empty today):
- One row-filter policy (`region = current_user_region()`).
- One column-mask policy using `CASE WHEN ... THEN col ELSE mask(col) END`.
- One policy using a supported ACL function.
- One policy using an unsupported UDF to exercise the gap-5 fallback.

Test matrix: full-access user (baseline), restricted user (row filter reduces count, column mask applied), unsupported-UDF query (expected `NOT_SUPPORTED` or fallback), EventListener emits all audit fields. Runs against `apache/ranger:2.4.0` already in `docker/docker-compose.yml:49`.

**Gap 9 — Operator documentation** (unblocked)
Runbook covering: supported policy idioms with examples, unsupported UDFs + fallback behavior, user-facing error messages, per-query opt-out via `SET SESSION prism.acceleration_enabled=false`.

### Implementation order and agent slicing

| Order | Agent | Gap | Size | Depends on |
|---|---|---|---|---|
| 1 | E | Gap 1 + 2 (session identity + ACL functions) | M | — |
| 2 | F | Gap 3 (`IfThen` consumer + tests) | S | — |
| 3 | G | Gap 4 (string kernel audit + fill) | S | — |
| 4 | H | Gap 5 + 9 (UDF fallback + docs) | M | E, F, G |
| 5 | I | Gap 7 (EventListener audit fields) | S | — |
| 6 | — | Gap 8 (integration tests) | S | E, F, G, H, I |
| 7 | — | Gap 6 (connector hooks) | S | Item 1 (upstream Trino SPI PR) |

Agents E, F, G, I are independent and can land in parallel. H and the integration test pass are sequential after. Gap 6 is the only piece gated on metadata delegation.

**Total size:** 3–4 weeks for gaps 1–5 and 7–9; gap 6 adds ~1 week once item 1's SPI PR ships.
