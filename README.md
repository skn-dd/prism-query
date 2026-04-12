# Prism

Arrow-native SIMD execution engine for Trino.

Prism forks Trino and replaces the JVM-heap Page/Block execution model with Apache Arrow RecordBatch operations in Rust. The result: vectorized SIMD execution for the hot-path operators (join, aggregation, filter, sort, string ops) while keeping Trino's mature SQL semantics, optimizer, and connector ecosystem.

## Architecture

```
Trino Coordinator (Java)
  SQL Parser → Analyzer → Planner → Optimizer
         ↓ Substrait IR
  Substrait Translation Layer
         ↓ Arrow Flight (gRPC)
Prism Workers (Hybrid)
  Java Shell (Connectors, Ranger ACL, JNI Bridge)
  ↔ Native Executor (Rust/Arrow SIMD kernels)
         ↕ Arrow Flight (worker ↔ worker)
```

**What's in Rust:** hash join, hash aggregation, filter/project, sort, string operations, Arrow Flight shuffle.

**What stays in Java:** SQL parsing, query planning, cost optimizer, connector SPI, Ranger access control, session management.

## Key Features

- **Arrow-native execution** — RecordBatch operations with SIMD kernels, not JVM object layouts
- **Arrow Flight shuffle** — Zero-copy inter-worker data transfer replacing REST-based exchange
- **Substrait** — Open interchange format for query plans between Java planner and Rust executor
- **OSI support** — Open Semantic Interchange model definitions for semantic layer queries
- **Apache Ranger** — Built-in access control (row/column level) via Trino's Ranger plugin

## Project Structure

```
prism/
├── native/               # Rust workspace
│   ├── prism-executor/   # Arrow SIMD operators (join, agg, filter, sort, strings)
│   ├── prism-flight/     # Arrow Flight shuffle writer/reader
│   ├── prism-substrait/  # Substrait plan consumer
│   ├── prism-osi/        # OSI semantic model parser
│   └── prism-jni/        # JNI bridge (cdylib → libprism_jni.so)
├── trino/                # Trino fork (Java)
│   └── prism-bridge/     # JNI wrappers + Substrait serializer
├── proto/                # Protobuf definitions
│   ├── substrait/        # Substrait protos
│   └── prism/            # Prism execution protos
├── osi-models/           # Example OSI model definitions
├── docker/               # Container builds + compose
└── docs/                 # Architecture documentation
```

## Building

### Native (Rust)

```bash
cd native
cargo build --release

# Run tests
cargo test --workspace

# Run benchmarks
cargo bench -p prism-executor
```

### Java (prism-bridge)

```bash
cd trino/prism-bridge
mvn package
```

### Docker

```bash
docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml up
```

## Operators Replaced

| Trino Operator | Prism Rust Replacement | Benefit |
|---|---|---|
| `DefaultPagesHash` (hash join) | `prism-executor/hash_join.rs` | Arrow dictionary encoding + SIMD gather |
| `FlatHash` (hash aggregation) | `prism-executor/hash_aggregate.rs` | Vectorized accumulation on columnar arrays |
| `ScanFilterAndProjectOperator` | `prism-executor/filter_project.rs` | SIMD bitmasking for predicate evaluation |
| `OrderByOperator` | `prism-executor/sort.rs` | Radix sort on primitives, merge sort on strings |
| `VariableWidthBlock` string ops | `prism-executor/string_ops.rs` | SSE4.2/AVX2 byte scanning |

## Standards Support

- **Substrait** — Query plan interchange format (protobuf). Trino plans are serialized to Substrait by the Java `SubstraitSerializer`, then deserialized by the Rust `prism-substrait` consumer.
- **OSI** — Open Semantic Interchange v0.1.1. Semantic model definitions (YAML/JSON) are parsed by `prism-osi` and exposed as virtual catalogs.
- **Apache Ranger** — Access control policies. Built into Trino since v480, configured via `access-control.name=ranger`.

## License

Apache License 2.0
