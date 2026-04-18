# Prism Architecture Overview

_Status: reflects the code as of 2026-04-18. Companion to `architecture.md` (target design) and `production-roadmap.md` (gap analysis)._

## 1. What Prism Is Today

Prism is a Trino extension that pushes a **narrow, specific workload** — TPC-H style scans/aggregations/joins against local Parquet — out of Trino's JVM into a native Rust execution engine. It is **not** currently a general-purpose acceleration layer for arbitrary Trino queries.

Two processes cooperate:

| Process | Language | Binary | Role |
|---|---|---|---|
| Trino server | Java | `trino-server-468` | SQL parsing, planning, optimization, catalog routing, JVM operator execution |
| Prism worker | Rust | `prism-bench` | Arrow Flight gRPC server; Substrait plan executor over Arrow RecordBatches |

They are linked by a Trino connector plugin (`prism-bridge`) that emits **Substrait** plan bytes and ships them to the worker via **Arrow Flight**.

## 2. Component Map

```
repo/
├── native/                     # Rust workspace
│   ├── prism-bench/            # Standalone worker binary (Flight server + Parquet loader)
│   ├── prism-executor/         # Arrow-native physical operators (scan, filter, agg, join, sort)
│   ├── prism-substrait/        # Substrait proto → internal PlanNode tree
│   ├── prism-flight/           # Arrow Flight gRPC server plumbing
│   ├── prism-jni/              # JNI entry points (unused in production path today)
│   └── prism-osi/              # OSI semantic-layer model loader
│
├── trino/prism-bridge/         # Trino connector plugin (Java)
│   └── src/main/java/io/prism/plugin/
│       ├── PrismConnectorFactory.java   # Reads prism.workers=host:port list
│       ├── PrismMetadata.java           # Hardcoded TPCH_TABLES (lineitem, orders)
│       ├── PrismPageSource.java         # Substrait serialize → Flight call → Arrow → Page
│       ├── SubstraitSerializer.java     # PlanNode → Substrait proto
│       ├── PrismFlightExecutor.java     # Flight client (DoAction / DoPut / DoGet)
│       ├── ArrowPageConverter.java      # Arrow RecordBatch → Trino Page (one-way only)
│       └── PrismNativeExecutor.java     # JNI wrapper (not wired into prod path)
│
├── proto/                      # Substrait + Prism custom protobufs
├── osi-models/                 # Example semantic-layer YAML models
└── docs/
    ├── architecture.md              # Target design (includes OSI, Ranger, shuffle)
    ├── architecture-overview.md     # This file (current state)
    ├── production-roadmap.md        # Gap analysis and phased plan
    ├── substrait-mapping.md
    ├── osi-integration.md
    └── deployment.md
```

## 3. Query Execution Flow (Today)

### 3a. Query against `prism.tpch.lineitem` (accelerated path)

```
Client SQL
  │
  ▼
Trino Coordinator (JVM)
  │  parse → analyze → plan → optimize
  │  PrismMetadata.applyFilter/applyAggregation/applyJoin pushes operators
  │  into a PrismPlanNode tree owned by the connector
  ▼
Trino Worker (JVM)
  │  PrismPageSource.getNextPage() invoked
  │  SubstraitSerializer.serialize(plan) → protobuf bytes
  │  Build command JSON: { substrait_plan_b64, tables: {"lineitem": "tpch/lineitem"} }
  ▼
Arrow Flight (gRPC) ─────────────────────────────────►  Prism Rust Worker (prism-bench)
                                                          │  DoAction("execute")
                                                          │  load_tables_smart() resolves
                                                          │    "tpch/lineitem" → Parquet files
                                                          │    under --data-dir on local disk
                                                          │  prism-substrait: proto → PlanNode
                                                          │  prism-executor: vectorized ops
                                                          ▼
                          Arrow IPC batches  ◄─────────  DoGet(Ticket)
  │
  ▼
ArrowPageConverter: Arrow → Trino Page/Block
  │
  ▼
Results to client
```

Four workers run on ports 50051–50054. `PrismPageSource` fan-outs across workers by assigning each worker a disjoint Parquet partition under `/data/prism/worker{0..3}/tpch/...`.

### 3b. Query against `oracle.*` joined with `snowflake.*` (standard Trino path)

```
Client SQL
  │
  ▼
Trino Coordinator (JVM)
  │  Standard cross-catalog plan; no Prism involvement
  ▼
Trino Workers (JVM)
  │  OracleConnector  → JDBC pull → Trino Page/Block
  │  SnowflakeConnector → JDBC pull → Trino Page/Block
  │  HashJoinOperator (JVM) joins the two streams
  ▼
Results to client
```

**The Prism Rust workers are idle** for this query. Zero bytes flow through them. The `prism-bridge` plugin is not consulted because no table in the query lives in the `prism` catalog.

## 4. Why the Federated Case Doesn't Reach Prism

Three hard-coded assumptions bind Prism to the local-Parquet world:

1. **Catalog scope.** `PrismMetadata.getTableHandle()` returns non-null only for schema `"tpch"` in the `prism` catalog. Anything else is invisible to the connector, so Trino never asks Prism to participate.
2. **Table naming.** `PrismMetadata.TPCH_TABLES` is a hardcoded 2-entry `Map<String, List<ColumnDef>>` with fixed schemas. No dynamic schema discovery.
3. **Worker data source.** `parquet_scan.rs` uses `std::fs` only. The worker has no JDBC driver, no object store client, no network fetcher. Its `load_tables_smart()` resolves every table name to `{--data-dir}/{store_key}/` on local disk (or to an in-memory `PartitionStore` populated via `DoPut`).

The in-memory `PartitionStore` + `DoPut` path is the escape hatch. It already allows the Java side to push arbitrary Arrow IPC batches into a worker and have them execute a Substrait plan against that data. It just isn't driven from any non-`prism` execution path today.

## 5. Integration Points That Would Need to Change for Oracle + Snowflake

To make a federated Oracle×Snowflake join run on Prism workers, the following five changes are required. This matches the Phase 5 "Path B: JNI Execution Bridge" section of `production-roadmap.md`.

| # | Change | Location | Exists Today? |
|---|---|---|---|
| 1 | Reverse Page→Arrow converter (JDBC rows → Arrow IPC) | `ArrowPageConverter.java` | No — only Arrow→Page exists |
| 2 | Interceptor in Trino's operator pipeline to route compute out of JVM | New `PrismOperatorFactory` in `prism-bridge` | No — requires touching Trino internals, not public SPI |
| 3 | Drive `DoPut` from the interceptor to push JDBC rows into a named in-memory partition | `PrismFlightExecutor.sendData()` | Method exists; caller does not |
| 4 | Plan rewrite: build Substrait plan referencing those in-memory table keys | `SubstraitSerializer` + new planner hook | Serializer exists; hook does not |
| 5 | (Alternative) wire the JNI bridge into the non-Prism path for in-process acceleration | `PrismNativeExecutor.executePlan()` | JNI entry exists; never called in prod |

**The hardest piece is #2.** Trino's `ConnectorSPI` only fires for queries rooted in the connector's own catalog. Intercepting execution on *other* catalogs requires reaching into `io.trino.operator`, which is not a public extension point. The roadmap scopes this at "L" (2–4 weeks) and P1 priority.

## 6. Substrait + Arrow Flight: the Seams

- **Substrait** is the serialization format for the plan. `SubstraitSerializer.java` walks a Trino `PlanNode` tree and emits `io.substrait.proto.Plan` bytes. `prism-substrait` in Rust deserializes it back into an internal `PlanNode` tree. This is the control plane between Java and Rust.
- **Arrow Flight** is the data plane. `DoAction("execute")` submits a plan + table manifest, `DoPut` pushes input batches, `DoGet(Ticket)` streams result batches. All payloads are Arrow IPC — zero deserialization overhead when converting on the Java side.

Both are standard open formats, which is what makes it feasible to bolt new data sources onto the Rust worker without changing the Java↔Rust contract.

## 7. Features Described in `architecture.md` That Are Not Yet Wired

The companion `architecture.md` describes the target design. The following pieces from it are **not** in the current execution path:

- **Java↔Rust JNI bridge in the production path.** The JNI entry exists (`prism-jni`) but production uses Arrow Flight (out-of-process), not JNI (in-process).
- **Mixed native/JVM execution with automatic fallback.** `SubstraitSerializer.isNativeSupported()` exists but is only consulted inside the `prism` catalog's own pushdown logic, not as a global operator-by-operator router.
- **Arrow Flight shuffle replacing Trino's `ExchangeOperator`.** The Flight server exists per-worker for result delivery, but inter-worker shuffle still uses Trino's native exchange.
- **OSI semantic layer at query time.** `osi-models/` and `prism-osi/` exist; there is no `osi.*` catalog resolver in the coordinator path.
- **Ranger policy enforcement.** Relies on Trino 480+ built-in support; not actively configured in the benchmark environment.

## 8. Known Structural Limits

- `Trino PushAggregationIntoTableScan` rule only matches `step=SINGLE` aggregates, which blocks aggregation pushdown for join queries (see memory: "Join query bottleneck at SF100"). This is why the 8-query bench excludes the join query.
- Schema evolution for pushdown tables is manual — new tables require editing `TPCH_TABLES` and redeploying the plugin jar.
- Worker failure handling is minimal; a worker crash during `DoAction("execute")` fails the query.

## 9. One-Line Summary

Today Prism is a Trino *catalog* backed by a Rust Arrow engine over local Parquet. Making it a Trino *accelerator* for cross-catalog queries (Oracle, Snowflake, Hive, etc.) requires adding a Page→Arrow converter, a non-SPI operator interceptor, and dynamic table registration on the worker — none of which exist yet, but all of which build on infrastructure (Substrait, Flight, `DoPut`, JNI) that is already in place.
