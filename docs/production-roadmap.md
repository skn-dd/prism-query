# Prism Production Roadmap

## Current State

Prism is a hybrid Java/Rust query engine built as a Trino connector plugin. It offloads compute-intensive operators (filter, project, aggregate, join, sort, TopN) to Rust workers using Apache Arrow SIMD kernels, connected via Arrow Flight gRPC and Substrait protobuf plans.

### What works today

| Capability | Status |
|---|---|
| Filter pushdown (comparison, AND/OR/NOT, IS NULL, LIKE) | Done |
| Projection pushdown (column pruning, computed expressions) | Done |
| Aggregation pushdown (SUM, COUNT, AVG, MIN, MAX, COUNT_DISTINCT) | Done |
| Join pushdown (Inner, Left, Right, Full, LeftSemi, LeftAnti) | Done |
| TopN pushdown (ORDER BY + LIMIT) | Done |
| Limit pushdown (pure LIMIT without ORDER BY) | Done |
| Multi-worker fan-out with partial aggregate merge | Done |
| AVG decomposition (SUM/COUNT) for distributed correctness | Done |
| $cast unwrap for mixed-type arithmetic | Done |
| Date32 type end-to-end (filter, compare, Substrait, datagen) | Done |
| LIKE/ILIKE predicate via SIMD string kernels | Done |
| Parquet reader with row group statistics skipping | Done |
| Arrow Flight shuffle (zero-copy gRPC transport) | Done |
| Substrait plan exchange (Java serialize, Rust consume) | Done |
| JNI bridge (Java -> Rust) for embedded execution | Done |

### Architecture: Two-Process Model (current)

Prism currently runs as **two separate sets of processes**:

1. **Trino JVM cluster** (coordinator + workers) — standard Trino, unmodified. Handles SQL parsing, planning, CBO optimization, and all connector execution (tpch, tpcds, hive, postgres, etc.)
2. **Rust worker processes** (standalone binaries on ports 50051/50052) — receive Substrait plans via Arrow Flight, execute with SIMD kernels, return Arrow results

The Prism connector plugin (`connector.name=prism`) runs inside the Trino JVM workers. When a query targets the `prism` catalog, the plugin serializes the plan and sends it to the separate Rust workers. Queries targeting any other catalog (hive, postgres, etc.) go through standard Trino JVM execution — they never touch the Rust workers.

```
Client SQL
  → Trino Coordinator (Java)
    → Trino Worker (Java)
      → prism catalog?  ──→ PrismPageSource → Substrait → Arrow Flight → Rust Worker
      → hive catalog?   ──→ Standard HiveConnector (JVM Page/Block execution)
      → postgres?       ──→ Standard JdbcConnector (JVM Page/Block execution)
```

**Implications:**
- Cross-catalog federation works (Trino coordinator orchestrates both sides)
- BUT only the `prism` catalog queries get Rust SIMD acceleration
- Existing connectors (Postgres, Hive, Iceberg, Kafka, etc.) run at standard Trino JVM speed
- The Rust workers have their own data (local Parquet / in-memory). They cannot access data from other Trino connectors.

### Architecture: Target Production Model

For production, Prism needs to evolve from "additional catalog" to "accelerated execution engine" so that real customer workloads benefit from Rust execution. There are three paths:

**Path A — Connector Delegation (recommended for data lake workloads, ~80% of queries):**
Prism reads Iceberg/Delta/Parquet from S3 directly in Rust. Metadata comes from Hive Metastore / Glue / Iceberg REST Catalog. No standard Trino data connector needed — Prism handles both metadata and execution.

```
prism.catalog-type=iceberg
prism.iceberg.catalog=glue
prism.iceberg.warehouse=s3://customer-warehouse/
```

**Path B — JNI Execution Bridge (for JDBC federation, ~20% of queries):**
For Postgres/MySQL sources, let Trino's standard JDBC connector scan the remote DB, convert the Page output to Arrow IPC, pass to Rust via the existing JNI bridge (`prism-jni`) for heavy operators (join, aggregate, sort), return results as Arrow IPC. The JNI entry point exists (`executePlan(handle, substraitBytes, inputIPC) → outputIPC`) but is not wired into the non-Prism execution path.

**Path C — Full Engine Replacement (long-term):**
Replace Trino's `OperatorFactory`/`Driver` pipeline so ALL operators (from any connector) execute on Arrow RecordBatches in Rust. This is the deepest integration but requires changes to Trino's internal execution model, not just the public connector SPI.

---

## Phase 1 — Read Real Data

**Goal:** Customers can point Prism at their existing Iceberg/Delta tables on S3 and run queries.

### 1.1 Cloud Object Storage (S3, GCS, HDFS)

**Gap:** All file I/O uses `std::fs` (local filesystem). No customer has data on local worker disks.

**Work:**
- Add `object_store` crate with S3/GCS/Azure feature flags
- Replace `std::fs::File::open` in `parquet_scan.rs` with `object_store::ObjectStore` async reads
- Support `s3://bucket/path`, `gs://bucket/path`, `abfss://container@account/path` URIs
- Credential propagation: Trino coordinator passes IAM role / service account to workers via task metadata
- Connection pooling for object store clients

**Files:** `native/prism-executor/src/parquet_scan.rs`, `native/Cargo.toml`, `native/prism-bench/src/handler.rs`

### 1.2 Parquet Column Pruning at I/O Level

**Gap:** Parquet reader reads ALL columns from selected row groups, then prunes in memory. Wastes I/O bandwidth — critical for wide tables (100+ columns) over network storage.

**Work:**
- Apply `ProjectionMask` on `ParquetRecordBatchReaderBuilder` based on the query's projected columns
- Thread column indices from `PlanNode::Scan { projection }` through to the Parquet reader
- Expected impact: 5-20x I/O reduction for selective queries on wide tables

**Files:** `native/prism-executor/src/parquet_scan.rs`

### 1.3 Iceberg Table Format

**Gap:** No table format support. Customers use Iceberg for schema evolution, time travel, partition pruning, and ACID guarantees.

**Work:**
- Integrate `iceberg-rust` crate for metadata parsing
- Read metadata JSON/Avro -> manifest list -> manifest files -> data file discovery
- Partition pruning: use partition values in manifests to skip irrelevant data files
- Schema evolution: handle column add/drop/rename between snapshots
- Time travel: resolve snapshot IDs for `AS OF TIMESTAMP` / `AS OF VERSION` queries
- Support Iceberg REST Catalog, Hive Metastore, and Glue Catalog as metadata sources

**Files:** New `native/prism-iceberg/` crate, integration into `parquet_scan.rs`

### 1.4 Delta Lake Table Format

**Gap:** No Delta Lake support.

**Work:**
- Integrate `deltalake` crate for transaction log parsing
- Read `_delta_log/` JSON + checkpoint Parquet -> active file list
- Partition pruning from add action metadata
- Schema evolution from Delta schema metadata
- Support Unity Catalog as metadata source

**Files:** New `native/prism-delta/` crate

### 1.5 Dynamic Catalog / Metastore Integration

**Gap:** Table schemas are hardcoded in `PrismMetadata.java` (`TPCH_TABLES` map). Adding a table requires recompiling.

**Work:**
- Replace `TPCH_TABLES` with a catalog provider interface
- Implement Hive Metastore (Thrift) client for table/partition discovery
- Implement AWS Glue Catalog client
- Implement Iceberg REST Catalog client
- Schema loaded at query time from metastore, not compile time
- Configuration: `prism.catalog-type=hive-metastore`, `prism.metastore.uri=thrift://host:9083`

**Files:** `trino/prism-bridge/src/main/java/io/prism/plugin/PrismMetadata.java`, new `CatalogProvider` interface

---

## Phase 2 — Don't Crash at Scale

**Goal:** Prism handles production-scale data (TB+) without OOM, with correct results for financial workloads.

### 2.1 Memory Tracking and Budgets

**Gap:** `getMemoryUsage()` returns hardcoded `0`. Trino's backpressure system depends on connectors reporting memory usage accurately. No OOM protection.

**Work:**
- Implement `MemoryPool` with configurable per-worker budget (e.g., `prism.max-memory=8GB`)
- Track allocations in hash tables (join, aggregate), sort buffers, and Parquet batch buffers
- Report via `TaskMetrics.peak_memory_bytes` (proto field already exists)
- Wire into Trino's `SystemMemoryUsageListener` via the page source

**Files:** New `native/prism-executor/src/memory.rs`, `hash_join.rs`, `hash_aggregate.rs`, `PrismPageSource.java`

### 2.2 Spill-to-Disk

**Gap:** Hash join, hash aggregate, and sort all build unbounded in-memory data structures. A single large GROUP BY or JOIN will OOM the worker.

**Work:**
- **External sort:** Merge-sort with spilling to temporary files when input exceeds memory budget
- **Grace hash join:** Partition build + probe sides to disk by hash, process partition-at-a-time, recursively spill if a partition still exceeds budget
- **Aggregate spill:** When hash table exceeds threshold, flush partial aggregates to disk, merge at end
- Configuration: `prism.spill-path=/tmp/prism-spill`, `prism.spill-threshold=0.8`

**Files:** `native/prism-executor/src/sort.rs`, `hash_join.rs`, `hash_aggregate.rs`, new `native/prism-executor/src/spill.rs`

### 2.3 Decimal128 Type Fidelity

**Gap:** All numeric aggregation casts to `f64`, losing precision. Financial workloads (SUM of money columns) produce wrong results.

**Work:**
- Add `ScalarValue::Decimal128(i128, u8, i8)` (value, precision, scale)
- Thread Decimal through filter comparisons, aggregation accumulators, and scalar expressions
- Use Arrow's native Decimal128 compute kernels instead of f64 cast
- Handle Trino's Decimal → Substrait Decimal → Arrow Decimal128 end-to-end

**Files:** `filter_project.rs`, `hash_aggregate.rs`, `consumer.rs`, `SubstraitSerializer.java`

### 2.4 COUNT_DISTINCT Performance

**Gap:** Uses `Vec<u64>` with linear `.contains()` — O(n^2) for high-cardinality columns.

**Work:**
- Replace with `HashSet<u64>` (one-line fix, massive impact)

**Files:** `hash_aggregate.rs`

### 2.5 Date32 Row Group Statistics

**Gap:** Date predicates don't trigger row group skipping in Parquet because `stats_might_match()` doesn't handle Date32.

**Work:**
- Add Date32 case to `stats_might_match()` using the underlying i32 representation

**Files:** `parquet_scan.rs`

---

## Phase 3 — Secure and Reliable

**Goal:** Prism is deployable in enterprise environments with security, fault tolerance, and operational tooling.

### 3.1 TLS + Authentication

**Gap:** Flight server is plaintext (`forGrpcInsecure`), handshake returns `Status::unimplemented`. Zero security.

**Work:**
- TLS on Flight gRPC endpoints (rustls or native-tls on Rust side, Netty SSL on Java side)
- Bearer token authentication: coordinator generates signed JWT, workers validate
- mTLS option for service-to-service auth in zero-trust environments
- Configuration: `prism.tls.enabled=true`, `prism.tls.keystore-path`, `prism.tls.truststore-path`

**Files:** `native/prism-bench/src/main.rs` (Flight server setup), `PrismFlightExecutor.java` (Flight client)

### 3.2 Worker Health Checks and Retry

**Gap:** If any worker fails, the entire query fails immediately. No retry, no failover, no health monitoring.

**Work:**
- Periodic heartbeat from coordinator to workers (reuse Flight `DoAction("ping")`)
- Mark workers as unhealthy after N missed heartbeats
- Retry failed worker tasks on healthy workers with exponential backoff
- Configurable: `prism.worker-heartbeat-interval=10s`, `prism.max-retries=3`

**Files:** `PrismPageSource.java`, `PrismConnector.java` (worker health tracker)

### 3.3 Service Discovery

**Gap:** Workers are a static comma-separated string in properties file. No dynamic scaling.

**Work:**
- Kubernetes: workers register as a headless Service, coordinator discovers via DNS SRV
- Implement the `WorkerRegistration` proto (already defined in `execution.proto` but not implemented)
- Workers self-register on startup, deregister on shutdown
- Coordinator watches for membership changes

**Files:** `PrismConnectorFactory.java`, Rust worker `main.rs`, `execution.proto`

### 3.4 Query Timeout and Cancellation

**Gap:** No timeout configuration. `CancelTask` proto defined but not implemented.

**Work:**
- Implement `CancelTask` DoAction handler on Rust workers
- Set per-query timeout propagated from Trino session properties
- Clean up in-flight work on cancellation (drop Parquet readers, free hash tables)

**Files:** Rust Flight handler, `PrismPageSource.java`

---

## Phase 4 — Feature Complete

**Goal:** Prism handles the full spectrum of interactive analytics SQL.

### 4.1 Window Functions

**Gap:** No `PlanNode::Window`. `ROW_NUMBER()`, `RANK()`, `SUM() OVER(...)` are heavily used.

**Work:**
- Add `PlanNode::Window` with partition-by keys, order-by keys, and frame spec
- Consume Substrait `ConsistentPartitionWindowRel`
- Implement partition/sort-based frame evaluation with Arrow arrays
- Support: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, SUM/AVG/MIN/MAX OVER

**Files:** New `native/prism-executor/src/window.rs`, `consumer.rs`, `plan.rs`, `PrismMetadata.java`

### 4.2 Intra-Query Parallelism

**Gap:** Execution within a single worker is single-threaded (one batch at a time through `execute_node()`).

**Work:**
- Parallel Parquet file scanning: multiple files read concurrently via Tokio spawn
- Parallel hash probe: partition probe side, process partitions on separate threads
- Pipeline breakers (sort, aggregate) remain synchronization points

**Files:** `executor.rs`, `parquet_scan.rs`, `hash_join.rs`

### 4.3 Data Distribution and Partitioned Splits

**Gap:** Split manager creates one split pointing to worker 0. No data locality.

**Work:**
- Hash/range partitioning of data across workers
- Split-based parallelism: each worker reads its assigned partitions
- Coordinator assigns splits based on data locality (same AZ, same rack)
- Consistent hashing for partition assignment to handle worker adds/removes

**Files:** `PrismSplitManager.java`, Rust worker data loading

### 4.4 Set Operations (UNION / INTERSECT / EXCEPT)

**Work:**
- Add `PlanNode::SetOperation` with UNION ALL (append), UNION (deduplicate), INTERSECT, EXCEPT
- Consume Substrait `SetRel`

### 4.5 Cross Join and Non-Equi Join

**Work:**
- Add nested-loop join for cross joins and theta joins
- Fall back to nested-loop when `extract_join_keys()` finds no equi-join conditions

### 4.6 Timestamp and Complex Types

**Work:**
- `ScalarValue::Timestamp(i64)` for TimestampMicrosecond
- `ScalarValue::Interval` for interval arithmetic
- List/Struct/Map support in filter, projection, and aggregation

---

## Phase 5 — Connector Federation (Critical Architecture Decision)

**Goal:** Make Prism the execution engine for ALL queries, not just queries against the `prism` catalog.

### The Problem

Today, Prism is a connector — it only accelerates queries targeting its own catalog. A customer's existing workloads against Hive, Iceberg, Postgres, etc. still run through Trino's standard JVM execution at standard Trino speed. They get zero benefit from Prism unless they explicitly migrate queries to the `prism` catalog.

This is the central architectural question for production: **Is Prism a new catalog, or is it a new execution engine?**

### Path A: Delegating Connector (recommended first step)

Prism wraps existing data sources — it handles metadata by delegating to Hive Metastore / Glue / Iceberg REST Catalog, but does execution in Rust:

```properties
# Customer configures prism to read their existing Iceberg tables
prism.catalog-type=iceberg
prism.iceberg.catalog=glue
prism.iceberg.warehouse=s3://customer-warehouse/

# Or Hive Metastore
prism.catalog-type=hive
prism.hive.metastore.uri=thrift://metastore:9083
```

Customer writes `SELECT * FROM prism.analytics.events` — Prism resolves the table via Glue, reads Parquet from S3 in Rust, executes with SIMD. **Same data, same table format, different execution engine.**

**Covers:** ~80% of production workloads (data lake: Iceberg, Delta, Hive-managed Parquet on S3/GCS).

**Does NOT cover:** JDBC federation (Postgres, MySQL, etc.) — those sources push data to Trino, they can't be read directly from Rust.

### Path B: JNI Execution Bridge (for JDBC federation)

For Postgres/MySQL and other push-based sources, intercept the data flow between Trino's connector and its execution operators:

```
Standard Trino:  JdbcConnector → Page → JVM Operators → Page → Result
With Prism:      JdbcConnector → Page → [Arrow IPC via JNI] → Rust Operators → [Arrow IPC] → Page → Result
```

The JNI bridge already exists (`prism-jni/src/lib.rs: executePlan(handle, substraitBytes, inputIPC) → outputIPC`). What's missing is:
1. A `PrismOperatorFactory` that plugs into Trino's execution pipeline for non-Prism catalogs
2. Page → Arrow IPC conversion at the boundary
3. Operator selection: route compute-heavy operators (aggregate, join, sort) through Rust, keep simple scans in Java

**Covers:** The remaining ~20% (JDBC sources, Kafka, Elasticsearch, etc.)

### Path C: Full Engine Replacement (long-term vision)

Replace Trino's `PageProcessor` / `Driver` pipeline entirely with Arrow-based execution. Every operator for every connector runs on Arrow RecordBatches in Rust. This requires either:
- Deep modification to Trino's internal execution model (not public SPI), or
- Forking Trino's execution layer

This is the highest-performance option but the highest risk. Consider only after Paths A+B are proven.

### Recommendation

1. **Ship Path A first** (Phase 1 + Phase 5A combined) — this IS Phase 1, since the Iceberg/Delta/S3 work makes Prism a delegating connector by definition
2. **Build Path B** when customers need cross-source federation (Prism Iceberg JOIN Postgres)
3. **Evaluate Path C** based on production experience — it may not be necessary if Path A covers enough workloads

### JDBC Federation Work (Path B)

| Task | Description |
|---|---|
| `PrismOperatorFactory` | Trino SPI hook that intercepts operator creation for heavy operators |
| Page→Arrow converter | Efficient Page/Block → Arrow RecordBatch conversion (already partially exists in `ArrowPageConverter.java`, needs reverse direction) |
| Selective routing | Only route aggregate/join/sort to Rust; leave scans and simple filters in Java |
| Mixed-engine query plan | Coordinator marks operator boundaries where Java↔Rust handoff occurs |

---

## Priority Matrix

| Phase | Item | Effort | Impact | Priority |
|---|---|---|---|---|
| 1 | S3/Object Storage | L | Blocker | P0 |
| 1 | Parquet column pruning | S | High perf | P0 |
| 1 | Iceberg reader + catalog delegation | XL | Blocker | P0 |
| 1 | Dynamic catalog (Glue/HMS) | L | Blocker | P0 |
| 1 | Delta Lake reader | L | High reach | P1 |
| 2 | Memory tracking | M | Stability | P0 |
| 2 | Spill-to-disk | XL | Stability | P1 |
| 2 | Decimal128 | M | Correctness | P1 |
| 2 | COUNT_DISTINCT fix | XS | Correctness | P0 |
| 2 | Date32 row group stats | XS | Perf | P1 |
| 3 | TLS + Auth | L | Security | P0 |
| 3 | Worker health + retry | M | Reliability | P1 |
| 3 | Service discovery | M | Operations | P2 |
| 3 | Query timeout + cancel | S | Operations | P2 |
| 4 | Window functions | L | Feature gap | P1 |
| 4 | Intra-query parallelism | L | Perf | P2 |
| 4 | Partitioned splits | L | Scalability | P2 |
| 4 | UNION/INTERSECT/EXCEPT | M | Feature gap | P2 |
| 5A | Iceberg/Delta delegation | — | (included in Phase 1) | P0 |
| 5B | JNI execution bridge for JDBC federation | L | Federation | P1 |
| 5C | Full engine replacement | XL | Long-term | P3 |

**Effort:** XS (<1 day), S (1-3 days), M (1-2 weeks), L (2-4 weeks), XL (1-2 months)
