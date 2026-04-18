# Prism Production Plan

_Companion to `production-roadmap.md` (gap analysis) and `architecture-overview.md` (current state). This doc sequences the work from "accelerates local Parquet" to "production-deployable Trino acceleration layer for cloud data lakes."_

_Last updated: 2026-04-18._

## Strategic Direction

1. **Extend Prism to read all open file formats across all major object stores** — AWS S3, Azure ADLS, GCS, OCI. This is where Rust/Arrow acceleration has clear, measurable value (compute-bound columnar scans, filter/aggregate pushdown).
2. **Defer JDBC federation acceleration (Oracle, Snowflake, Postgres) until a profiled workload proves JVM operators are the bottleneck.** JDBC paths are usually bound by wire protocol and source engine, not Trino execution. Engineering cost (non-SPI operator interceptor, Page↔Arrow round-trip) is high relative to expected gain.
3. **Production readiness is not optional.** A working engine without deployment, observability, or security is a demo, not a product. Phases 2–9 are load-bearing.

## Suggested Sequencing

1 → 2 → 6 → 4 → 3 → 5 → 7 → 8 → 9.

Observability (Phase 6) moves up because you cannot operate what you cannot see. Phase 1 alone is 6–10 weeks; full production readiness is realistically 4–6 months of focused work.

---

## Phase 1 — Multi-Format, Multi-Cloud Reader

**Goal:** A customer can point Prism at their Iceberg/Delta/Parquet tables in any major object store and run queries with Rust/Arrow acceleration.

### 1.1 Rust worker: object-store abstraction

- Replace `std::fs` calls in `native/prism-bench/src/parquet_scan.rs:4` with the `object_store` crate. One crate covers S3, GCS, Azure Blob / ADLS Gen2, and OCI (via S3-compat).
- Update `native/prism-bench/src/handler.rs` `load_tables_smart()` (lines 312–388) to dispatch on a URI scheme (`s3://`, `gs://`, `abfs://`, `oci://`, `file://`) rather than assuming a local path prefix.
- Cloud credential providers via workload identity only — no static access keys in configs:
  - AWS: IRSA (IAM Roles for Service Accounts).
  - GCP: GKE Workload Identity.
  - Azure: AAD Pod Identity / Workload Identity.
  - OCI: Instance principals.
- Break-glass fallback: credentials from a mounted secret (for non-K8s deployments).

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

### 1.4 Worker config file

- The 2-flag CLI (`--port`, `--data-dir`) does not scale to cloud configuration. Introduce a TOML config at `/etc/prism/worker.toml` covering: bind address, object-store credentials, memory budget, TLS, telemetry endpoints, log level.
- CLI flags override file values; env vars override both (standard precedence).

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

**Rough size:** 6–10 weeks.

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

### Changes
- **mTLS on Arrow Flight:** rustls server + Netty client. Certs provisioned via cert-manager. Rotate via Kubernetes pod restart.
- **Trino HTTPS + OAuth/OIDC:** already supported by Trino; wire it up in the Helm chart and document provider setup.
- **Ranger integration finalized:** remove the hardcoded password from compose; provision via secret. Move Ranger out of `security` profile.
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
