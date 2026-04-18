# Prism Production Plan

_Companion to `production-roadmap.md` (gap analysis) and `architecture-overview.md` (current state). This doc sequences the work from "accelerates local Parquet" to "production-deployable Trino acceleration layer for cloud data lakes."_

_Last updated: 2026-04-18._

## Strategic Direction

1. **Extend Prism to read all open file formats across all major object stores** — AWS S3, Azure ADLS, GCS, OCI. This is where Rust/Arrow acceleration has clear, measurable value (compute-bound columnar scans, filter/aggregate pushdown).
2. **Defer JDBC federation acceleration (Oracle, Snowflake, Postgres) until a profiled workload proves JVM operators are the bottleneck.** JDBC paths are usually bound by wire protocol and source engine, not Trino execution. Engineering cost (non-SPI operator interceptor, Page↔Arrow round-trip) is high relative to expected gain.
3. **Production readiness is not optional.** A working engine without deployment, observability, or security is a demo, not a product. Phases 2–9 are load-bearing.
4. **Integrate with Trino's ecosystem; do not build parallel universes.** For every cross-cutting concern — credentials, authentication, authorization, audit, session properties, resource groups, retry — use Trino's existing mechanism. Prism adds acceleration; it does not re-implement operational plumbing.

## Trino-Ecosystem Integration Principles

This is the load-bearing constraint for every design decision below. Whenever we face a choice between "build it in Prism" or "wire it to Trino's existing facility," we choose the latter by default.

| Concern | Trino mechanism | Prism posture |
|---|---|---|
| **Catalog metadata** (schemas, tables, columns) | Delegated connector's `ConnectorMetadata` SPI, configured via catalog properties files | Delegate (Phase 1.3). Do not re-implement Iceberg/Hive/Delta metadata. |
| **Object-store credentials** | Catalog properties (e.g. `hive.s3.iam-role`), AWS SDK default chain, workload identity | Read from the delegated catalog's properties; ship only the resolved/ephemeral credentials to the Rust worker in `DoAction` over mTLS. Never build a parallel credential store. |
| **Per-user / per-query credentials** | `X-Trino-Extra-Credential` header → `ConnectorSession.getIdentity().getExtraCredentials()` | Plumb through to worker for connectors that need them. No Prism-specific mechanism. |
| **Authentication** | Trino authenticator plugins (OAuth2, JWT, Kerberos, password) | Identity arrives via `ConnectorSession.getIdentity()`. No Prism auth layer. |
| **Authorization / access control** | `SystemAccessControl` SPI, Ranger plugin (built into Trino 480+) | Policies apply at plan phase, before Prism sees the query. Row filters and column masks flow through `ConnectorMetadata` hooks. Inherit free via delegation. |
| **Audit log** | `EventListener` SPI | Emit Prism-specific fields (bytes scanned, worker ID, acceleration status) via the plugin's `EventListener`. No separate audit stream. |
| **Session properties / tunables** | `Connector.getSessionProperties()` | Register `prism.acceleration_enabled`, `prism.memory_budget_gb`, etc. as Trino session properties. Not env vars, not TOML for per-query knobs. |
| **Resource groups** | Trino resource group manager | Applied at coordinator; Prism queries governed identically to non-Prism queries. Nothing to build. |
| **Fault-tolerant execution** | Trino FTE | Compatible provided `PrismPageSource` is idempotent on retry. Verify in Wave 2. |
| **Query cancellation** | Trino query lifecycle → connector `close()` | Connector plumbs cancel through to `DoAction("cancel", queryId)`. No separate control plane. |
| **Observability (JVM side)** | Trino JMX + query events | Prism plugin exposes metrics via standard JMX. Rust worker adds its own Prometheus endpoint (separate process — unavoidable). |
| **Worker-process config** | N/A (Prism-specific) | `worker.toml` is legitimately Prism territory: bind address, memory, logging, credentials-cache TTL. It must NOT carry catalog config or session tunables. |
| **Inter-worker shuffle** | Trino `ExchangeManager` SPI | Prism's Arrow Flight shuffle currently runs outside this SPI — Trino loses visibility for FTE, spill, and exchange metrics. Rewire to register as a Trino `ExchangeManager` plugin (see Phase 1.5 below). |
| **Error surfaces** | `TrinoException` + `StandardErrorCode` | Plugin currently throws `PrismExecutionException extends RuntimeException`. Map to `TrinoException` at the boundary for proper error codes in query results. |

**Before starting any new slice, ask: "is there a Trino-native mechanism for this?" If yes, use it.**

## Suggested Sequencing

1 → 7-mTLS-subset → 2 → 6 → 4 → 3 → 5 → 7 → 8 → 9.

Observability (Phase 6) moves up because you cannot operate what you cannot see. The mTLS-on-Flight subset of Phase 7 moves up as a Wave 2 prerequisite (D3 credential transport requires it). Phase 1 alone is 6–10 weeks; full production readiness is realistically 4–6 months of focused work.

## Status — Wave 1 complete (2026-04-18)

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

---

## Phase 1 — Multi-Format, Multi-Cloud Reader

**Goal:** A customer can point Prism at their Iceberg/Delta/Parquet tables in any major object store and run queries with Rust/Arrow acceleration.

### 1.1 Rust worker: object-store abstraction

- Replace `std::fs` calls in `native/prism-bench/src/parquet_scan.rs:4` with the `object_store` crate. One crate covers S3, GCS, Azure Blob / ADLS Gen2, and OCI (via S3-compat).
- Update `native/prism-bench/src/handler.rs` `load_tables_smart()` (lines 312–388) to dispatch on a URI scheme (`s3://`, `gs://`, `abfs://`, `oci://`, `file://`) rather than assuming a local path prefix.
- Credential resolution follows the ecosystem principle: **the delegated Trino catalog's properties file is the source of truth.** The Rust worker does not independently resolve credentials from the default provider chain for delegated tables.
  - Coordinator-side code reads the delegated catalog's `hive.s3.*` / `iceberg.rest.*` / Glue / Azure / GCS properties at query time.
  - Ephemeral credentials (STS, short-lived tokens) are minted via the catalog's configured identity and shipped to the worker in the `DoAction` payload over **mTLS**. Static credentials are never wire-transported.
  - `ConnectorSession.getIdentity().getExtraCredentials()` (from `X-Trino-Extra-Credential` header) is plumbed through for per-user creds.
  - Workload identity (IRSA / GKE WI / Azure WI / OCI instance principals) is a fallback for clusters that don't configure per-catalog credentials — it can't honor differentiated catalog configs, so it's not the primary path.
- **This requires mTLS on Flight** as a prerequisite. See Phase 7 — mTLS moves up to Wave 2 prerequisite.

### 1.2 Rust worker: format expansion

Create `native/prism-executor/src/scans/` with one module per format:

| Format | Crate | Maturity |
|---|---|---|
| Parquet | `parquet` (existing) | Done — port from current `parquet_scan.rs` |
| Iceberg | `iceberg-rust` | Active Apache project; covers catalog + scan |
| Delta Lake | `delta-rs` | Mature |
| ORC | `arrow-orc` | Newer; validate read perf |
| CSV / JSON | arrow-rs built-ins | Trivial; include for completeness |

### 1.3 Metadata delegation (Path A from roadmap)

- **Do not duplicate Hive/Iceberg/Delta metadata resolution.** Let Trino's existing connectors own catalog lookup; Prism takes over the scan only. This avoids catalog drift and cuts massive amounts of code.
- Replace hardcoded `TPCH_TABLES` map in `PrismMetadata.java:26` with dynamic schema resolution via the delegated upstream connector.
- New plugin-side flow: Trino resolves `iceberg.analytics.orders` → Iceberg metadata → list of data files → Prism plugin detects files are in an object store we support → pushes scan to Rust worker → worker reads files directly.

**Design decisions (resolved):**

| # | Question | Decision |
|---|---|---|
| D1 | How does Prism reference the delegated catalog? | **Named reference**: `prism.delegate-catalog-name=my_iceberg` — the Iceberg catalog is configured once in Trino's normal `etc/catalog/iceberg.properties`, Prism reuses it. Avoids connector-lifecycle hazards and eliminates credential duplication. |
| D2 | Worker protocol for tables | **Version the `DoAction` schema to accept URI lists** per table, not a single path string. Iceberg/Delta manifests produce file lists, not directories. |
| D3 | Credential handoff to worker | Three-tier: (a) resolved config from delegated catalog's properties, (b) ephemeral/STS tokens minted coordinator-side per query, (c) `ExtraCredentials` pass-through for per-user creds. All shipped over mTLS in the `DoAction` payload. Workload identity is a fallback only. |
| D4 | Table handle key | **Composite `(catalog, schema, table)`** — table names are no longer globally unique after delegation. Cascades through `equals`/`hashCode`. |
| D5 | Trino version coupling | **SPI-only.** Use `ConnectorManager` / named-catalog lookup to find the delegated catalog at runtime. Do not bundle `trino-iceberg` JAR. Works against whatever Iceberg/Delta version the cluster already runs. |

**Prerequisites that land in Wave 2 alongside delegation:**

- **mTLS on Flight** (promoted from Phase 7) — required for D3 credential transport.
- **`ExtraCredentials` plumbing** — pull `session.getIdentity().getExtraCredentials()` through `PrismFlightExecutor.executeQuery()` into the Rust worker.
- **`EventListener` integration** — emit Prism-specific fields (`acceleration_used`, `bytes_scanned`, `worker_ids`, `rust_runtime_ms`) via the Trino event stream. No separate audit pipe.
- **Session property registration** — register `prism.acceleration_enabled`, `prism.memory_budget_gb` via `Connector.getSessionProperties()`.

### 1.4 Worker config file

- The 2-flag CLI (`--port`, `--data-dir`) does not scale to worker-process configuration. Introduce a TOML config at `/etc/prism/worker.toml` covering: bind address, memory budget, TLS certs/keys, telemetry endpoints, log level, credential-cache TTLs.
- CLI flags override file values; env vars override both (standard precedence).
- **Scope boundary:** `worker.toml` is for the worker process only — bind address, memory, logging, TLS material. It must **not** carry catalog credentials, table definitions, or per-query tunables. Catalog config lives in Trino catalog properties (delegated); per-query tunables live in Trino session properties. _Status: merged (commit `2c861cc`/`d21cd24`)._
- **Gate:** reserved TOML sections `[object_store.s3]`, `[object_store.gcs]`, `[object_store.azure]` exist as placeholders in `deploy/worker.toml.example`. Do **not** implement these — if implemented they'd become parallel catalog credential config. Credentials flow through the delegated catalog (see D3), not through `worker.toml`. Leave the example comments in place as a reminder, but strike them from the struct when they would otherwise be added.

### 1.5 Shuffle: rewire to Trino's `ExchangeManager` SPI

_Surfaced by Wave 1 parallel-universe audit._

- `prism-flight` currently implements its own Arrow Flight shuffle outside Trino's `ExchangeManager` SPI. It is operational (used by `PrismFlightExecutor` and `prism-bench`), not dead code. But Trino loses visibility: FTE can't retry, spill policies don't apply, exchange throughput doesn't appear in JMX.
- **Work:** register the Arrow Flight transport as an `ExchangeManager` plugin so Trino sees it as a first-class exchange backend. The transport implementation stays in `prism-flight`; only the Java registration layer is new.
- **Non-goal for Wave 2:** removing `prism-flight`. Keep the Rust transport; just put it under Trino's SPI umbrella.

### 1.6 Error mapping — `TrinoException` at the boundary

_Surfaced by Wave 1 parallel-universe audit._

- `PrismExecutionException extends RuntimeException` at `trino/prism-bridge/src/main/java/io/prism/bridge/PrismExecutionException.java:6` is thrown from `PrismFlightExecutor.java:116` and `PrismNativeExecutor.java:74`. Trino surfaces these as opaque internal errors.
- **Work:** at every user-visible boundary, wrap in `TrinoException(GENERIC_INTERNAL_ERROR, …)` (or a more specific `ErrorCode` where the failure type is known, e.g. `EXCEEDED_LOCAL_MEMORY_LIMIT`, `REMOTE_HOST_GONE`).
- Small, self-contained slice. Quick win for operators.

### Deliverables
- `native/prism-executor/src/scans/{parquet,iceberg,delta,orc,csv,json}.rs`
- `native/prism-bench/src/object_store.rs` (URI dispatcher)
- `native/prism-bench/src/config.rs` (TOML loader)
- Updated `PrismMetadata.java` with delegated metadata path
- Integration tests against MinIO (S3), fake-gcs-server, Azurite for per-cloud conformance

### Non-goals for Phase 1
- Hudi (lower priority, less customer demand than Iceberg/Delta).
- Write path. Prism is read-only for the foreseeable future.
- JDBC acceleration.

**Rough size:** 10–14 weeks total. Wave 1 (1.1 + 1.4) is complete. Wave 2 covers 1.2, 1.3, 1.5, 1.6 plus the pulled-up mTLS and `EventListener`/session-property ecosystem wiring.

---

## Phase 2 — Enablement Model on a Trino Cluster

**Goal:** Cluster operators have clear on/off controls at three levels of granularity.

| Knob | Location | Effect |
|---|---|---|
| **Install / uninstall** | Presence of `etc/catalog/prism-*.properties` | Plugin loaded or not |
| **Per-catalog enable** | `prism.acceleration=true\|false` in catalog properties | Catalog exists but routes through standard Trino execution when disabled |
| **Per-session kill switch** | `SET SESSION prism.acceleration_enabled=false` | Ops escape hatch during incidents |

### Deliverables
- `docker/etc/catalog/prism.properties.example` (currently missing) documenting every connector key
- Session property registration in the Trino plugin
- Per-catalog enable plumbed through `PrismConnectorFactory` / `PrismMetadata` so disabling bypasses pushdown without removing the catalog
- Admin docs: how to roll out, how to roll back, how to kill-switch

**Rough size:** 1 week.

---

## Phase 3 — Container Hardening

**Goal:** Production-grade container images.

Current state: `docker/Dockerfile.worker` and `Dockerfile.coordinator` work but aren't hardened.

### Changes
- Multi-arch builds: `linux/amd64` + `linux/arm64` (Graviton is meaningfully cheaper for scan-heavy workloads).
- Distroless base image for the worker (minimizes CVE surface).
- Pinned `rust-toolchain.toml` for reproducible builds.
- Read-only root filesystem, drop all Linux capabilities.
- gRPC health protocol endpoint on the worker (for K8s probes — see Phase 4).
- Image signing via cosign in CI; provenance attestations.
- Clear image tagging: `prism-worker:<git-sha>` and `:<semver>`; no `:latest`.

### Deliverables
- Updated Dockerfiles
- `.github/workflows/build-images.yml` (or equivalent) with multi-arch, signing
- `rust-toolchain.toml` committed at repo root

**Rough size:** 1–2 weeks.

---

## Phase 4 — Helm Chart + K8s Topology

**Goal:** `helm install prism` deploys a production-grade cluster on any major K8s distribution.

Current state: zero K8s scaffolding. Docker Compose only.

### Topology
- **Coordinator:** `Deployment` + `HorizontalPodAutoscaler` (stateless; scales on query concurrency or CPU).
- **Workers:** `StatefulSet` with a headless `Service`. StatefulSet gives stable DNS (`prism-worker-0.prism-worker.svc`), which the consistent-hash ring in Phase 5 depends on.
- **ServiceAccount** bound to cloud workload identity. No baked-in credentials in images or ConfigMaps.
- **ConfigMap** for Trino config + `etc/catalog/*`.
- **Secret** only for break-glass fallback credentials (prefer workload identity).
- **PodDisruptionBudget** to protect minimum worker count during rollouts.
- **NetworkPolicy:** only `coordinator → worker:50051`; deny all other worker ingress.
- Resource requests/limits with sane defaults (e.g. 4 CPU / 16 GB per worker).

### Deliverables
- `deploy/helm/prism/Chart.yaml`
- `deploy/helm/prism/values.yaml` with per-cloud presets (`--set cloud=aws|azure|gcp|oci`)
- `deploy/helm/prism/templates/` covering all resources above
- `helm test` hooks for basic smoke test post-install

**Rough size:** 2–3 weeks.

---

## Phase 5 — Discovery and Elastic Scaling

**Goal:** Workers can be added and removed without manual reconfiguration or query failures.

Current state: `prism.workers=localhost:50051,...` in `PrismConnectorFactory.java` is a static comma-separated list. Hardest single blocker to production scaling.

### Changes
- Accept `prism.workers=dns:///prism-worker.svc:50051` and resolve via gRPC's DNS resolver (optionally SRV records for port discovery).
- **Consistent-hash ring** on the coordinator side. Without this, every scale event re-partitions every in-flight query, which is unacceptable.
- Worker graceful drain: SIGTERM → stop accepting new `DoAction` requests, finish in-flight work, exit 0. PDB from Phase 4 respects this.
- `terminationGracePeriodSeconds` tuned to accommodate the longest acceptable query.

### Deliverables
- Connector-side resolver + hash ring
- Worker drain signal handling in `native/prism-bench/src/main.rs`
- Chaos test: scale up/down mid-query, verify no client-visible errors

**Rough size:** 2–3 weeks.

---

## Phase 6 — Observability

**Goal:** Every query and every worker is instrumented. Ops can diagnose issues in <5 minutes.

Current state: zero Prometheus metrics, no health endpoints, only `tracing` to stderr.

### Deliverables

**Metrics (Rust worker, via `prometheus` crate):**
- `prism_queries_total{status}` (counter)
- `prism_query_duration_seconds{operator}` (histogram)
- `prism_bytes_scanned_total{format,source}` (counter)
- `prism_rows_produced_total` (counter)
- `prism_active_queries` (gauge)
- `prism_flight_rpc_errors_total{code}` (counter)

**Tracing:**
- OpenTelemetry trace context propagated from Trino through Flight gRPC headers into Rust worker spans.
- Query ID as a span attribute end-to-end.

**Logging:**
- Structured JSON logs with query IDs, correlation IDs, tenant IDs if applicable.

**Dashboards + alerts:**
- Grafana JSON committed to `deploy/helm/prism/dashboards/`.
- Golden alerts: worker down, error rate >1% for 5m, p99 latency regression vs 1-hour baseline, OOM kills.

**Rough size:** 2 weeks.

---

## Phase 7 — Security

**Goal:** Wire-level encryption, identity-based auth, audit trail.

Current state: all plaintext. Ranger profile-gated with hardcoded admin password in compose.

### Wave 2 prerequisite subset (pulled up from Phase 7)

- **mTLS on Arrow Flight:** rustls server + Netty client. Certs provisioned via cert-manager. Rotate via Kubernetes pod restart. **This must land before the delegated-credential transport in D3 goes live** — the worker will start accepting ephemeral credentials in `DoAction` payloads and plaintext is not acceptable.

### Remaining Phase 7 changes

- **Trino HTTPS + OAuth/OIDC:** already supported by Trino; wire it up in the Helm chart and document provider setup.
- **Ranger integration finalized:** remove the hardcoded password from compose; provision via secret. Move Ranger out of `security` profile. (Note: audit confirms Prism uses Trino's native Ranger SPI — no duplication exists today, this is just hardening.)
- **Query audit log:** emit per-query who / what / when / bytes-scanned. Target S3 / CloudWatch / equivalent.

### Deliverables
- rustls wiring in `prism-flight`
- cert-manager Issuer templates in the Helm chart
- Ranger configuration docs and hardened compose
- Audit log sink with pluggable destinations

**Rough size:** 2–3 weeks.

---

## Phase 8 — Reliability

**Goal:** The cluster survives worker crashes, slow queries, and memory pressure without data loss or cluster-wide outages.

### Changes
- Retry/timeout in `PrismFlightExecutor` (currently no retry logic).
- Circuit breaker: a worker failing repeatedly is dropped from the pool and re-probed.
- Per-query memory budget on the worker (`prism.max_query_memory_gb`).
- Spill-to-disk for large aggregates / joins. Big piece; defer to Phase 8.1 if we accept fail-fast for v1.
- Query cancellation: Trino cancels → `DoAction("cancel", query_id)` → worker drops the task and releases resources.

### Deliverables
- Retry + circuit breaker in Java client
- Memory accounting + cancellation in Rust executor
- Fault injection tests (kill worker mid-query, starve memory, slow Flight RPC)

**Rough size:** 3–4 weeks (longer if spill-to-disk lands in v1).

---

## Phase 9 — CI/CD and Release

**Goal:** Every commit produces a reproducible, tested, signed artifact. Customers can upgrade safely.

### Deliverables
- Build matrix: Rust cross-compile for amd64+arm64; Trino 468 + 470 compatibility.
- Conformance suite: TPC-H, TPC-DS, Iceberg table-format tests.
- **Shadow/canary mode:** session flag runs N% of queries with acceleration off, compares results byte-for-byte. Catches correctness regressions before they hit production.
- SemVer release tags. `CHANGELOG.md` with breaking changes clearly marked.
- Upgrade runbook: rolling upgrade sequencing (coordinator-first vs worker-first, cross-version compatibility window).

**Rough size:** 2–3 weeks, ongoing maintenance thereafter.

---

## Post-v1: Deferred Items

These matter but should not block initial production rollout.

- **JDBC acceleration** (Oracle, Snowflake, Postgres): reopen only when profiled evidence shows JVM operator bottleneck on a real customer workload.
- **Hudi support.**
- **Write path** (INSERT / MERGE / DELETE). Read-first; writes are a fundamentally different problem.
- **Full engine replacement** (Path C from roadmap): replace Trino's `OperatorFactory` entirely. Years-scale undertaking; only consider if delegation pattern hits fundamental limits.

---

## Open Design Questions

1. **How does Prism coexist with Trino's own Iceberg/Delta connectors on the same cluster?** Do they live side-by-side under different catalog names, or does Prism transparently intercept scans on the existing catalogs? The latter is better UX but requires Phase 5-Path-B-style interception.
2. **Multi-tenancy model.** Per-tenant worker pools, or shared workers with resource quotas? Affects Phase 4 (topology) and Phase 8 (isolation).
3. **Caching layer.** Local SSD cache of hot Parquet pages dramatically helps re-read performance but adds a state dimension to workers (complicates Phase 5 scaling).

Resolve these before Phase 4 Helm chart freezes topology decisions.
