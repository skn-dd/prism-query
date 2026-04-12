# Substrait Mapping

Maps between Trino PlanNode types and Substrait relation types.

## Relation Mapping

| Trino PlanNode | Substrait Rel | Prism PlanNode | Notes |
|---|---|---|---|
| `TableScanNode` | `ReadRel` (NamedTable) | `Scan` | Projection pushed into `ReadRel.projection` |
| `FilterNode` | `FilterRel` | `Filter` | Predicate in `FilterRel.condition` |
| `ProjectNode` | `ProjectRel` | `Project` | Column refs in `ProjectRel.expressions` |
| `AggregationNode` | `AggregateRel` | `Aggregate` | Grouping + measures |
| `JoinNode` | `JoinRel` | `Join` | Equi-join keys extracted from expression |
| `SortNode` | `SortRel` | `Sort` | Sort fields with direction + nulls ordering |
| `TopNNode` | `SortRel` + fetch | `Sort` (with limit) | Limit encoded in plan metadata |
| `ExchangeNode` | `ExchangeRel` | `Exchange` | Arrow Flight endpoints in custom extension |

## Expression Mapping

| Trino Expression | Substrait Expression | Function Reference |
|---|---|---|
| `a = b` | `ScalarFunction(equal)` | 1 |
| `a != b` | `ScalarFunction(not_equal)` | 2 |
| `a < b` | `ScalarFunction(less_than)` | 3 |
| `a <= b` | `ScalarFunction(less_than_or_equal)` | 4 |
| `a > b` | `ScalarFunction(greater_than)` | 5 |
| `a >= b` | `ScalarFunction(greater_than_or_equal)` | 6 |
| `a AND b` | `ScalarFunction(and)` | 7 |
| `a OR b` | `ScalarFunction(or)` | 8 |
| `NOT a` | `ScalarFunction(not)` | 9 |
| Column ref | `FieldReference(DirectReference(StructField(n)))` | — |
| Literal int | `Literal(I64(n))` | — |
| Literal float | `Literal(Fp64(n))` | — |
| Literal string | `Literal(String(s))` | — |

## Aggregate Function Mapping

| Trino Aggregate | Substrait Function Reference |
|---|---|
| `count(col)` | 0 |
| `sum(col)` | 1 |
| `min(col)` | 2 |
| `max(col)` | 3 |
| `avg(col)` | 4 |
| `count(DISTINCT col)` | 5 |

## Type Mapping

| Trino Type | Substrait Type | Arrow DataType |
|---|---|---|
| `BOOLEAN` | `Bool` | `Boolean` |
| `INTEGER` | `I32` | `Int32` |
| `BIGINT` | `I64` | `Int64` |
| `REAL` | `Fp32` | `Float32` |
| `DOUBLE` | `Fp64` | `Float64` |
| `VARCHAR` | `String` | `Utf8` |
| `DATE` | `Date` | `Date32` |
| `TIMESTAMP` | `Timestamp` | `Timestamp(Microsecond, None)` |
| `DECIMAL(p,s)` | `Decimal(p,s)` | `Decimal128(p,s)` |
