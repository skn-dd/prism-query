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
