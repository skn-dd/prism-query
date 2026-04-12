# Prism Deployment Topology & Container Configuration

## Deployment Models

Prism supports three deployment topologies depending on scale:

---

### 1. Single-Node Development (docker-compose)

For local development, testing, and small datasets (< 10GB).

```
┌─────────────────────────────────────────────┐
│            Docker Host (dev laptop)          │
│                                              │
│  ┌──────────────┐  ┌──────────────────────┐ │
│  │ Coordinator   │  │ Worker (co-located)  │ │
│  │ :8080 (HTTP)  │──│ :8080 (HTTP)         │ │
│  │ Java 21       │  │ :50051 (Flight gRPC) │ │
│  │ 4GB heap      │  │ Java 21 + Rust libs  │ │
│  └──────────────┘  │ 8GB heap + 4GB native │ │
│                     └──────────────────────┘ │
│  ┌──────────────┐                            │
│  │ Ranger (opt) │                            │
│  │ :6080        │                            │
│  └──────────────┘                            │
└─────────────────────────────────────────────┘
```

```bash
# Quick start
docker compose up -d

# With Ranger access control
docker compose --profile security up -d
```

---

### 2. Multi-Node Cluster (Kubernetes / ECS)

Production deployment for medium-to-large workloads (10GB–10TB).

```
                    ┌──── Load Balancer (:443) ────┐
                    │     (ALB / Ingress)           │
                    └──────────┬───────────────────┘
                               │
              ┌────────────────┴────────────────┐
              ▼                                  │
┌─────────────────────────┐                      │
│    Coordinator (1x)     │                      │
│    c5.2xlarge / 8 vCPU  │                      │
│    Java 21, 16GB heap   │                      │
│    No native libs       │                      │
│    ─────────────────    │                      │
│    Roles:               │                      │
│    • SQL parse/plan     │                      │
│    • CBO optimization   │                      │
│    • Ranger ACL check   │                      │
│    • Task distribution  │                      │
│    • Result assembly    │                      │
│    ─────────────────    │                      │
│    Ports:               │                      │
│    • 8080 (HTTP API)    │                      │
│    • 8443 (HTTPS)       │                      │
└────────────┬────────────┘                      │
             │ Discovery (HTTP)                  │
     ┌───────┴────────┬──────────────┐           │
     ▼                ▼              ▼           │
┌──────────┐   ┌──────────┐   ┌──────────┐      │
│ Worker 1 │   │ Worker 2 │   │ Worker N │      │
│ r6i.4xl  │   │ r6i.4xl  │   │ r6i.4xl  │      │
│──────────│   │──────────│   │──────────│      │
│ Java 21  │   │ Java 21  │   │ Java 21  │      │
│ 24GB heap│   │ 24GB heap│   │ 24GB heap│      │
│──────────│   │──────────│   │──────────│      │
│ Rust     │   │ Rust     │   │ Rust     │      │
│ native   │   │ native   │   │ native   │      │
│ 8GB mmap │   │ 8GB mmap │   │ 8GB mmap │      │
│──────────│   │──────────│   │──────────│      │
│ Ports:   │   │ Ports:   │   │ Ports:   │      │
│ 8080 HTTP│   │ 8080 HTTP│   │ 8080 HTTP│      │
│ 50051 gRP│   │ 50051 gRP│   │ 50051 gRP│      │
└──────────┘   └──────────┘   └──────────┘      │
      │              │              │             │
      └──────────────┴──────────────┘             │
          Arrow Flight mesh (gRPC)                │
                                                  │
┌──────────────────┐  ┌───────────────────┐       │
│  Ranger Admin    │  │ Metastore (Hive)  │       │
│  (optional)      │  │ / Iceberg Catalog │       │
│  :6080           │  │ / Delta Catalog   │       │
└──────────────────┘  └───────────────────┘       │
                                                  │
┌──────────────────────────────────────────┐      │
│  Object Storage (S3 / GCS / ADLS / HDFS) │      │
│  Data lake with Parquet/ORC/Iceberg      │◄─────┘
└──────────────────────────────────────────┘
```

**Instance sizing guide:**

| Role | Instance | vCPU | RAM | Disk | Count |
|------|----------|------|-----|------|-------|
| Coordinator | c5.2xlarge | 8 | 16GB | 50GB SSD | 1 |
| Worker (compute) | r6i.4xlarge | 16 | 128GB | 200GB NVMe | 3–20 |
| Worker (memory-heavy) | r6i.8xlarge | 32 | 256GB | 500GB NVMe | for large joins |
| Ranger | t3.medium | 2 | 4GB | 20GB | 0–1 |

---

### 3. Hybrid Cloud (Kubernetes + Spot)

Cost-optimized for variable workloads using spot/preemptible workers.

```
┌─ On-Demand (stable) ────────────────────────┐
│  Coordinator (1x)                            │
│  Core Workers (2x) — always-on baseline      │
└──────────────────────────────────────────────┘
         │
┌─ Spot / Preemptible (elastic) ──────────────┐
│  Burst Workers (0–20x)                       │
│  Auto-scale on queue depth                   │
│  Graceful drain on spot interruption         │
└──────────────────────────────────────────────┘
```

---

## Container Configuration

### Worker Container — Memory Layout

Each worker container runs a hybrid JVM + native process:

```
Total Container Memory: 32GB (example r6i.4xlarge)
├── JVM Heap:           24GB  (JAVA_OPTS=-Xmx24G)
│   ├── Trino operators:     Used for Java-only operators
│   ├── Connector buffers:   Hive/Iceberg/Delta page cache
│   └── JNI byte[] bridge:   Arrow IPC transfer buffers
├── JVM Off-Heap:       2GB
│   ├── Direct ByteBuffers:  Arrow IPC zero-copy region
│   ├── Code cache:          512MB (JIT compiled)
│   └── Thread stacks:       ~200 threads × 1MB
├── Rust Native:        4GB
│   ├── Arrow memory pool:   RecordBatch allocations
│   ├── Hash tables:         Join build-side + aggregate accumulators
│   ├── Flight buffers:      gRPC send/receive queues
│   └── Sort scratch:        Temporary arrays during sort
└── OS / Container:     2GB
    ├── Filesystem cache
    └── gRPC/TLS overhead
```

### Environment Variables

```bash
# ─── JVM Configuration ──────────────────────────────────
JAVA_OPTS=-Xmx24G                    # JVM heap (60-75% of container RAM)

# ─── Native Executor ────────────────────────────────────
PRISM_NATIVE_LIB_PATH=/opt/prism/lib/native/libprism_jni.so
PRISM_NATIVE_MEMORY_LIMIT=4294967296  # 4GB Arrow memory pool limit
PRISM_NATIVE_SPILL_DIR=/tmp/prism     # Spill-to-disk directory
PRISM_NATIVE_SPILL_THRESHOLD=0.8      # Spill when 80% of native memory used

# ─── Arrow Flight ────────────────────────────────────────
PRISM_FLIGHT_PORT=50051               # gRPC port for shuffle
PRISM_FLIGHT_MAX_MESSAGE_SIZE=67108864  # 64MB max message (Arrow IPC batch)
PRISM_FLIGHT_THREADS=4                # gRPC server threads
PRISM_FLIGHT_TLS_CERT=/opt/prism/tls/server.crt  # optional TLS
PRISM_FLIGHT_TLS_KEY=/opt/prism/tls/server.key

# ─── OSI Semantic Layer ─────────────────────────────────
PRISM_OSI_MODEL_DIR=/opt/prism/models   # Directory for .osi.yaml files
PRISM_OSI_REFRESH_INTERVAL=300          # Hot-reload interval (seconds)

# ─── Ranger Access Control ──────────────────────────────
PRISM_RANGER_ENABLED=true
PRISM_RANGER_SERVICE_NAME=prism
PRISM_RANGER_POLICY_URL=http://ranger:6080
PRISM_RANGER_REFRESH_INTERVAL=30

# ─── Monitoring ──────────────────────────────────────────
PRISM_METRICS_PORT=9090               # Prometheus metrics endpoint
PRISM_TRACING_ENDPOINT=http://jaeger:14268/api/traces  # OpenTelemetry
```

### Kubernetes Deployment

```yaml
# coordinator-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prism-coordinator
  labels:
    app: prism
    role: coordinator
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prism
      role: coordinator
  template:
    metadata:
      labels:
        app: prism
        role: coordinator
    spec:
      containers:
        - name: coordinator
          image: prism-query/coordinator:latest
          ports:
            - containerPort: 8080
              name: http
          env:
            - name: JAVA_OPTS
              value: "-Xmx12G"
          resources:
            requests:
              cpu: "4"
              memory: "16Gi"
            limits:
              cpu: "8"
              memory: "16Gi"
          readinessProbe:
            httpGet:
              path: /v1/info
              port: 8080
            initialDelaySeconds: 30
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /v1/info
              port: 8080
            initialDelaySeconds: 60
            periodSeconds: 30
          volumeMounts:
            - name: osi-models
              mountPath: /opt/prism/models
            - name: catalog-config
              mountPath: /opt/prism/etc/catalog
      volumes:
        - name: osi-models
          configMap:
            name: prism-osi-models
        - name: catalog-config
          configMap:
            name: prism-catalogs
---
# worker-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: prism-worker
  labels:
    app: prism
    role: worker
spec:
  serviceName: prism-worker
  replicas: 3
  selector:
    matchLabels:
      app: prism
      role: worker
  template:
    metadata:
      labels:
        app: prism
        role: worker
    spec:
      containers:
        - name: worker
          image: prism-query/worker:latest
          ports:
            - containerPort: 8080
              name: http
            - containerPort: 50051
              name: flight-grpc
            - containerPort: 9090
              name: metrics
          env:
            - name: JAVA_OPTS
              value: "-Xmx24G"
            - name: PRISM_FLIGHT_PORT
              value: "50051"
            - name: PRISM_NATIVE_MEMORY_LIMIT
              value: "4294967296"
            - name: PRISM_NATIVE_SPILL_DIR
              value: "/data/spill"
          resources:
            requests:
              cpu: "8"
              memory: "32Gi"
            limits:
              cpu: "16"
              memory: "32Gi"
          readinessProbe:
            httpGet:
              path: /v1/info
              port: 8080
            initialDelaySeconds: 30
          volumeMounts:
            - name: spill-volume
              mountPath: /data/spill
            - name: catalog-config
              mountPath: /opt/prism/etc/catalog
      volumes:
        - name: catalog-config
          configMap:
            name: prism-catalogs
  volumeClaimTemplates:
    - metadata:
        name: spill-volume
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: fast-nvme
        resources:
          requests:
            storage: 200Gi
---
# services.yaml
apiVersion: v1
kind: Service
metadata:
  name: prism-coordinator
spec:
  selector:
    app: prism
    role: coordinator
  ports:
    - port: 8080
      name: http
  type: ClusterIP
---
apiVersion: v1
kind: Service
metadata:
  name: prism-worker
spec:
  selector:
    app: prism
    role: worker
  ports:
    - port: 8080
      name: http
    - port: 50051
      name: flight-grpc
  clusterIP: None  # Headless — workers discover each other by DNS
---
# HPA for workers
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: prism-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: prism-worker
  minReplicas: 2
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Pods
      pods:
        metric:
          name: prism_queued_queries
        target:
          type: AverageValue
          averageValue: "5"
```

### Network Topology

```
┌─────────────────────────────────────────────────────────────────┐
│                        Kubernetes Cluster                        │
│                                                                  │
│  ┌─────────── Service: prism-coordinator (ClusterIP) ─────────┐ │
│  │  :8080 → Coordinator Pod                                    │ │
│  │  External: via Ingress / ALB (:443)                         │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌─────────── Service: prism-worker (Headless) ───────────────┐ │
│  │  prism-worker-0.prism-worker:8080   (Trino HTTP)            │ │
│  │  prism-worker-0.prism-worker:50051  (Arrow Flight)          │ │
│  │  prism-worker-1.prism-worker:8080                           │ │
│  │  prism-worker-1.prism-worker:50051                          │ │
│  │  prism-worker-N.prism-worker:8080                           │ │
│  │  prism-worker-N.prism-worker:50051                          │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Traffic flows:                                                  │
│  1. Client → ALB → Coordinator:8080 (SQL queries)               │
│  2. Coordinator → Worker:8080 (task assignment, Trino protocol)  │
│  3. Worker ↔ Worker:50051 (Arrow Flight shuffle, gRPC)           │
│  4. Worker → S3/GCS (data lake reads via connector)              │
│  5. Coordinator → Ranger:6080 (policy sync)                     │
└─────────────────────────────────────────────────────────────────┘
```

### Port Reference

| Port | Protocol | Service | Direction | Purpose |
|------|----------|---------|-----------|---------|
| 8080 | HTTP | Trino | Client → Coord | SQL queries, REST API |
| 8443 | HTTPS | Trino | Client → Coord | TLS-encrypted queries |
| 50051 | gRPC | Arrow Flight | Worker ↔ Worker | Shuffle data transfer |
| 6080 | HTTP | Ranger | Coord → Ranger | Policy administration |
| 9090 | HTTP | Prometheus | Monitoring → All | Metrics scrape endpoint |
| 14268 | HTTP | Jaeger | All → Jaeger | Trace export |

### Security Configuration

```
# TLS between workers (Flight gRPC)
prism.flight.tls.enabled=true
prism.flight.tls.cert=/opt/prism/tls/server.crt
prism.flight.tls.key=/opt/prism/tls/server.key
prism.flight.tls.ca=/opt/prism/tls/ca.crt

# mTLS for inter-worker authentication
prism.flight.mtls.enabled=true
prism.flight.mtls.client-cert=/opt/prism/tls/client.crt
prism.flight.mtls.client-key=/opt/prism/tls/client.key

# Ranger access control
access-control.name=ranger
ranger.service-name=prism
ranger.policy-refresh-interval=30s
ranger.audit.enabled=true
```

---

## Production Checklist

- [ ] JVM heap ≤ 75% of container memory (leave room for native + OS)
- [ ] `PRISM_NATIVE_MEMORY_LIMIT` set explicitly (don't rely on defaults)
- [ ] NVMe spill volume mounted for large hash joins
- [ ] Arrow Flight port (50051) accessible between all workers
- [ ] TLS configured for Flight gRPC in production
- [ ] Ranger policies tested with least-privilege access
- [ ] Prometheus metrics enabled (`/v1/jmx` + `:9090/metrics`)
- [ ] HPA configured with both CPU and queue-depth metrics
- [ ] OSI models loaded from ConfigMap (not baked into image)
- [ ] Resource limits set on both coordinator and worker pods
