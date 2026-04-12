# Prism Architecture

## Overview

Prism is a hybrid Java/Rust query engine. It keeps Trino's coordinator and connector ecosystem in Java, but offloads compute-intensive operators to a native Rust execution engine using Apache Arrow.

## Data Flow

```
1. Client submits SQL query
   ↓
2. Trino Coordinator (Java)
   - Parses SQL (Antlr grammar)
   - Analyzes against catalog metadata
   - Plans and optimizes (CBO)
   - Checks Ranger access policies
   ↓
3. Substrait Translation
   - SubstraitSerializer.java walks PlanNode tree
   - Produces Substrait Plan (protobuf bytes)
   - Identifies which subtrees can run natively
   ↓
4. Task Assignment via Arrow Flight
   - Coordinator sends TaskAssignment to workers
   - Includes Substrait plan + split metadata
   ↓
5. Worker Execution (Rust)
   - prism-jni receives Substrait bytes via JNI
   - prism-substrait deserializes to PlanNode tree
   - prism-executor runs Arrow operators:
     • filter_project: SIMD predicate evaluation
     • hash_join: probe/build on Arrow RecordBatches
     • hash_aggregate: vectorized group-by
     • sort: radix/merge sort
     • string_ops: SIMD string matching
   ↓
6. Shuffle via Arrow Flight
   - prism-flight partitions output RecordBatches
   - Serves via Arrow Flight (gRPC)
   - Downstream stages pull zero-copy
   ↓
7. Results returned to coordinator → client
```

## JNI Bridge

The critical integration point between Java and Rust:

```
Java (PrismNativeExecutor.java)
  → System.loadLibrary("prism_jni")
  → init() → returns runtime handle
  → executePlan(handle, substraitBytes, inputIPC) → outputIPC
  → shutdown(handle)

Rust (prism-jni/src/lib.rs)
  → #[no_mangle] extern "system" JNI entry points
  → Deserializes Arrow IPC → RecordBatch
  → Consumes Substrait → PlanNode tree
  → Executes plan → RecordBatch stream
  → Serializes to Arrow IPC → returns to Java
```

Memory passes between Java and Rust as Arrow IPC byte arrays through JNI `byte[]`. The IPC format ensures compatible serialization without custom marshalling.

## Fallback Strategy

Not all Trino operators have native implementations. The execution strategy checks each subtree:

1. Walk the plan tree bottom-up
2. For each node, check `SubstraitSerializer.isNativeSupported()`
3. If all children are native-supported → execute entire subtree natively
4. If any child requires Java → split at the boundary:
   - Children execute in their native context
   - Results pass through Arrow IPC at the boundary
   - Parent executes in whichever context it supports

This means complex queries can mix native and Java execution within a single query.

## Arrow Flight Shuffle

Replaces Trino's REST-based `ExchangeOperator`:

### Trino (before)
- Output pages serialized to custom wire format
- Pulled via HTTP REST calls between workers
- Deserialized back into Page/Block on the receiving side

### Prism (after)
- Output RecordBatches partitioned by hash
- Served via Arrow Flight (gRPC streaming)
- Received as Arrow IPC — zero deserialization

The Flight server runs as a sidecar on each worker. Partition data is written to an in-memory store keyed by `stage_id/partition/N`. Remote workers connect with a `Ticket` containing the partition key.

## OSI Semantic Layer

OSI model files define a semantic layer over physical tables:

```yaml
datasets:
  - name: orders
    source: { catalog: hive, schema: analytics, table: orders }
    fields: [...]
    metrics:
      - name: total_revenue
        aggregate: sum
        field: amount
```

At query time:
1. User queries `SELECT total_revenue FROM osi.ecommerce.orders GROUP BY region`
2. Prism resolves `total_revenue` → `SUM(amount)` from the OSI model
3. Generates the SQL against the physical source table
4. Plans and executes as normal

## Apache Ranger Integration

Ranger provides centralized access control:

- **Authentication**: Handled by Trino's existing auth mechanisms
- **Authorization**: Ranger policies evaluated at planning time
- **Row-level security**: Ranger row-filter policies inject WHERE clauses
- **Column masking**: Ranger masking policies transform column expressions

Configuration is minimal since Trino has built-in Ranger support since v480.
