package io.prism.plugin;

import io.trino.spi.connector.*;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.*;

import java.util.*;

public class PrismMetadata implements ConnectorMetadata {

    // TPC-H table definitions matching datagen.rs schemas
    private static final Map<String, List<ColumnDef>> TPCH_TABLES = Map.of(
        "lineitem", List.of(
            new ColumnDef("l_orderkey", BigintType.BIGINT),
            new ColumnDef("l_partkey", BigintType.BIGINT),
            new ColumnDef("l_suppkey", BigintType.BIGINT),
            new ColumnDef("l_linenumber", IntegerType.INTEGER),
            new ColumnDef("l_quantity", DoubleType.DOUBLE),
            new ColumnDef("l_extendedprice", DoubleType.DOUBLE),
            new ColumnDef("l_discount", DoubleType.DOUBLE),
            new ColumnDef("l_tax", DoubleType.DOUBLE),
            new ColumnDef("l_returnflag", VarcharType.VARCHAR),
            new ColumnDef("l_linestatus", VarcharType.VARCHAR)
        ),
        "orders", List.of(
            new ColumnDef("o_orderkey", BigintType.BIGINT),
            new ColumnDef("o_custkey", BigintType.BIGINT),
            new ColumnDef("o_orderstatus", VarcharType.VARCHAR),
            new ColumnDef("o_totalprice", DoubleType.DOUBLE),
            new ColumnDef("o_orderpriority", VarcharType.VARCHAR)
        )
    );

    record ColumnDef(String name, Type type) {}

    static List<ColumnDef> getTableColumns(String tableName) {
        return TPCH_TABLES.get(tableName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return List.of("tpch");
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion) {
        if (!"tpch".equals(tableName.getSchemaName())) {
            return null;
        }
        if (!TPCH_TABLES.containsKey(tableName.getTableName())) {
            return null;
        }
        return new PrismTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        PrismTableHandle handle = (PrismTableHandle) table;
        List<ColumnDef> defs = TPCH_TABLES.get(handle.getTableName());
        if (defs == null) return null;

        List<ColumnMetadata> columns = defs.stream()
                .map(d -> new ColumnMetadata(d.name(), d.type()))
                .toList();

        return new ConnectorTableMetadata(
                new SchemaTableName(handle.getSchemaName(), handle.getTableName()),
                columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        PrismTableHandle handle = (PrismTableHandle) table;
        List<ColumnDef> defs = TPCH_TABLES.get(handle.getTableName());
        if (defs == null) return Map.of();

        Map<String, ColumnHandle> result = new LinkedHashMap<>();
        for (int i = 0; i < defs.size(); i++) {
            ColumnDef def = defs.get(i);
            result.put(def.name(), new PrismColumnHandle(def.name(), i, def.type()));
        }
        return result;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column) {
        PrismColumnHandle col = (PrismColumnHandle) column;
        return new ColumnMetadata(col.getColumnName(), col.getType());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        if (schemaName.isPresent() && !"tpch".equals(schemaName.get())) {
            return List.of();
        }
        return TPCH_TABLES.keySet().stream()
                .map(t -> new SchemaTableName("tpch", t))
                .toList();
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        List<TableColumnsMetadata> result = new ArrayList<>();
        for (var entry : TPCH_TABLES.entrySet()) {
            SchemaTableName name = new SchemaTableName("tpch", entry.getKey());
            if (prefix.matches(name)) {
                List<ColumnMetadata> cols = entry.getValue().stream()
                        .map(d -> new ColumnMetadata(d.name(), d.type()))
                        .toList();
                result.add(TableColumnsMetadata.forTable(name, cols));
            }
        }
        return result.iterator();
    }

    // ===== Filter Pushdown =====

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint) {
        PrismTableHandle handle = (PrismTableHandle) table;

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll() || summary.isNone()) {
            return Optional.empty();
        }

        Optional<Map<ColumnHandle, Domain>> domains = summary.getDomains();
        if (domains.isEmpty()) {
            return Optional.empty();
        }

        // Convert TupleDomain to PrismPlanNode predicates
        PrismPlanNode.PredicateNode predicate = null;
        Map<ColumnHandle, Domain> unhandled = new LinkedHashMap<>();

        for (var entry : domains.get().entrySet()) {
            PrismColumnHandle col = (PrismColumnHandle) entry.getKey();
            Domain domain = entry.getValue();

            PrismPlanNode.PredicateNode colPredicate = convertDomain(col, domain);
            if (colPredicate == null) {
                unhandled.put(entry.getKey(), domain);
                continue;
            }

            predicate = (predicate == null) ? colPredicate
                    : new PrismPlanNode.PredicateNode.And(predicate, colPredicate);
        }

        if (predicate == null) {
            return Optional.empty();
        }

        // Build plan tree: wrap current plan with Filter
        PrismPlanNode currentPlan = handle.getPushedPlan()
                .orElseGet(() -> buildFullScan(handle.getTableName()));
        PrismPlanNode newPlan = new PrismPlanNode.Filter(currentPlan, predicate);
        PrismTableHandle newHandle = handle.withPushedPlan(newPlan);

        TupleDomain<ColumnHandle> remaining = unhandled.isEmpty()
                ? TupleDomain.all()
                : TupleDomain.withColumnDomains(unhandled);

        return Optional.of(new ConstraintApplicationResult<>(newHandle, remaining, io.trino.spi.expression.Constant.TRUE, false));
    }

    private PrismPlanNode.PredicateNode convertDomain(PrismColumnHandle col, Domain domain) {
        if (domain.isAll() || domain.isNone()) {
            return null;
        }

        ValueSet values = domain.getValues();
        if (!(values instanceof SortedRangeSet rangeSet)) {
            return null;
        }

        List<Range> ranges = rangeSet.getOrderedRanges();
        if (ranges.isEmpty()) {
            return null;
        }

        PrismPlanNode.PredicateNode result = null;
        for (Range range : ranges) {
            PrismPlanNode.PredicateNode rangePred = convertRange(col, range);
            if (rangePred == null) continue;

            result = (result == null) ? rangePred
                    : new PrismPlanNode.PredicateNode.Or(result, rangePred);
        }

        return result;
    }

    private PrismPlanNode.PredicateNode convertRange(PrismColumnHandle col, Range range) {
        int colIdx = col.getColumnIndex();
        String valueType = typeToValueType(col.getType());

        if (range.isSingleValue()) {
            Object value = convertTrinoValue(col.getType(), range.getSingleValue());
            return new PrismPlanNode.PredicateNode.Comparison(colIdx, "EQUAL", value, valueType);
        }

        PrismPlanNode.PredicateNode low = null;
        PrismPlanNode.PredicateNode high = null;

        if (!range.isLowUnbounded()) {
            String op = range.isLowInclusive() ? "GREATER_THAN_OR_EQUAL" : "GREATER_THAN";
            Object value = convertTrinoValue(col.getType(), range.getLowBoundedValue());
            low = new PrismPlanNode.PredicateNode.Comparison(colIdx, op, value, valueType);
        }

        if (!range.isHighUnbounded()) {
            String op = range.isHighInclusive() ? "LESS_THAN_OR_EQUAL" : "LESS_THAN";
            Object value = convertTrinoValue(col.getType(), range.getHighBoundedValue());
            high = new PrismPlanNode.PredicateNode.Comparison(colIdx, op, value, valueType);
        }

        if (low != null && high != null) {
            return new PrismPlanNode.PredicateNode.And(low, high);
        }
        return low != null ? low : high;
    }

    private Object convertTrinoValue(Type type, Object value) {
        if (value == null) return null;
        if (type instanceof DoubleType) {
            if (value instanceof Double) return value;
            if (value instanceof Long) return Double.longBitsToDouble((Long) value);
            if (value instanceof Number) return ((Number) value).doubleValue();
            return value;
        }
        if (type instanceof RealType) {
            if (value instanceof Float) return value;
            if (value instanceof Long) return Float.intBitsToFloat(((Long) value).intValue());
            if (value instanceof Number) return ((Number) value).floatValue();
            return value;
        }
        if (type instanceof VarcharType) {
            if (value instanceof io.airlift.slice.Slice) {
                return ((io.airlift.slice.Slice) value).toStringUtf8();
            }
            return value.toString();
        }
        if (type instanceof IntegerType) {
            if (value instanceof Long) return ((Long) value).intValue();
            return value;
        }
        return value;
    }

    private String typeToValueType(Type type) {
        if (type instanceof BigintType) return "BIGINT";
        if (type instanceof IntegerType) return "INTEGER";
        if (type instanceof DoubleType) return "DOUBLE";
        if (type instanceof BooleanType) return "BOOLEAN";
        if (type instanceof VarcharType) return "STRING";
        return "STRING";
    }

    // ===== Projection Pushdown =====
    // Captures computed expressions (e.g. l_extendedprice * (1 - l_discount)) that Trino
    // decomposes out of aggregate arguments. Creates virtual columns so applyAggregation
    // can reference them as simple Variables.

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments) {
        PrismTableHandle handle = (PrismTableHandle) table;

        List<ConnectorExpression> outputProjections = new ArrayList<>();
        Map<String, Assignment> outputAssignmentMap = new LinkedHashMap<>();
        List<PrismPlanNode.ScalarExprNode> computedExprs = new ArrayList<>();

        int baseColumnCount = getTableColumns(handle.getTableName()).size();
        boolean hasComputedExprs = false;

        for (ConnectorExpression projection : projections) {
            if (projection instanceof Variable var) {
                ColumnHandle ch = assignments.get(var.getName());
                if (ch == null) {
                    return Optional.empty();
                }
                outputProjections.add(projection);
                outputAssignmentMap.putIfAbsent(var.getName(),
                        new Assignment(var.getName(), ch, var.getType()));
            } else if (projection instanceof Call call) {
                PrismPlanNode.ScalarExprNode exprNode = convertCallExpression(call, assignments);
                if (exprNode == null) {
                    return Optional.empty();
                }

                int virtualIdx = baseColumnCount + computedExprs.size();
                String virtualName = "__expr_" + computedExprs.size();
                PrismColumnHandle virtualCol = new PrismColumnHandle(virtualName, virtualIdx, projection.getType());

                outputProjections.add(new Variable(virtualName, projection.getType()));
                outputAssignmentMap.put(virtualName,
                        new Assignment(virtualName, virtualCol, projection.getType()));
                computedExprs.add(exprNode);
                hasComputedExprs = true;
            } else {
                return Optional.empty();
            }
        }

        if (!hasComputedExprs) {
            return Optional.empty();
        }

        // Build Project plan: passthrough all base columns + computed expressions
        PrismPlanNode currentPlan = handle.getPushedPlan()
                .orElseGet(() -> buildFullScan(handle.getTableName()));

        List<Integer> passthrough = new ArrayList<>();
        for (int i = 0; i < baseColumnCount; i++) {
            passthrough.add(i);
        }
        PrismPlanNode projectPlan = new PrismPlanNode.Project(currentPlan, passthrough, computedExprs);
        PrismTableHandle newHandle = handle.withPushedPlan(projectPlan);

        // Include all base columns in output assignments (subsequent pushdowns need them)
        List<ColumnDef> defs = getTableColumns(handle.getTableName());
        for (int i = 0; i < defs.size(); i++) {
            ColumnDef def = defs.get(i);
            outputAssignmentMap.putIfAbsent(def.name(),
                    new Assignment(def.name(), new PrismColumnHandle(def.name(), i, def.type()), def.type()));
        }

        return Optional.of(new ProjectionApplicationResult<>(
                newHandle,
                outputProjections,
                new ArrayList<>(outputAssignmentMap.values()),
                false));
    }

    // ===== Aggregation Pushdown =====
    // Single-split mode: one worker fully aggregates, Trino receives pre-aggregated results.

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets) {
        PrismTableHandle handle = (PrismTableHandle) table;

        // Only handle single grouping set (no ROLLUP/CUBE)
        if (groupingSets.size() != 1) {
            return Optional.empty();
        }
        List<ColumnHandle> groupByColumns = groupingSets.get(0);

        // Deduplicate group-by columns (Trino may pass duplicates)
        List<ColumnHandle> dedupedGroupBy = new ArrayList<>();
        Set<String> seenNames = new LinkedHashSet<>();
        for (ColumnHandle ch : groupByColumns) {
            PrismColumnHandle col = (PrismColumnHandle) ch;
            if (seenNames.add(col.getColumnName())) {
                dedupedGroupBy.add(ch);
            }
        }
        groupByColumns = dedupedGroupBy;

        // Convert group-by to column indices
        List<Integer> groupByIndices = new ArrayList<>();
        for (ColumnHandle ch : groupByColumns) {
            PrismColumnHandle col = (PrismColumnHandle) ch;
            groupByIndices.add(col.getColumnIndex());
        }

        // Convert aggregates — handle both simple column refs and expression args
        List<PrismPlanNode.AggregateExpr> aggExprs = new ArrayList<>();
        List<PrismPlanNode.ScalarExprNode> computedExprs = new ArrayList<>();
        int baseColumnCount = getTableColumns(handle.getTableName()).size();

        for (AggregateFunction agg : aggregates) {
            // Skip aggregates with filters or ordering (can't push those down)
            if (agg.getFilter().isPresent() || !agg.getSortItems().isEmpty()) {
                return Optional.empty();
            }

            String funcName = mapAggFunction(agg.getFunctionName(), agg.isDistinct());
            if (funcName == null) {
                return Optional.empty();
            }

            // Resolve input column — supports Variables and Call expressions
            int inputCol = 0;
            if (!agg.getArguments().isEmpty()) {
                ConnectorExpression input = agg.getArguments().get(0);
                if (input instanceof Variable var) {
                    ColumnHandle ch = assignments.get(var.getName());
                    if (ch instanceof PrismColumnHandle pch) {
                        inputCol = pch.getColumnIndex();
                    }
                } else if (input instanceof Call call) {
                    PrismPlanNode.ScalarExprNode exprNode = convertCallExpression(call, assignments);
                    if (exprNode == null) {
                        return Optional.empty();
                    }
                    inputCol = baseColumnCount + computedExprs.size();
                    computedExprs.add(exprNode);
                } else {
                    return Optional.empty();
                }
            }

            aggExprs.add(new PrismPlanNode.AggregateExpr(funcName, inputCol, "agg_" + aggExprs.size()));
        }

        // Build plan: if computed expressions exist, insert Project before Aggregate
        PrismPlanNode currentPlan = handle.getPushedPlan()
                .orElseGet(() -> buildFullScan(handle.getTableName()));
        if (!computedExprs.isEmpty()) {
            List<Integer> passthrough = new ArrayList<>();
            for (int i = 0; i < baseColumnCount; i++) passthrough.add(i);
            currentPlan = new PrismPlanNode.Project(currentPlan, passthrough, computedExprs);
        }
        PrismPlanNode aggPlan = new PrismPlanNode.Aggregate(currentPlan, groupByIndices, aggExprs);
        PrismTableHandle newHandle = handle.withPushedPlan(aggPlan);

        // Build output: projections (one per aggregate), assignments (all output cols),
        // groupingColumnMapping (new group-by handle → original handle)
        List<ConnectorExpression> projections = new ArrayList<>();
        List<Assignment> outputAssignments = new ArrayList<>();
        Map<ColumnHandle, ColumnHandle> groupingColumnMapping = new LinkedHashMap<>();

        // Group-by columns come first in the output
        int outputIdx = 0;
        for (ColumnHandle ch : groupByColumns) {
            PrismColumnHandle col = (PrismColumnHandle) ch;
            PrismColumnHandle outputCol = new PrismColumnHandle(col.getColumnName(), outputIdx, col.getType());
            outputAssignments.add(new Assignment(col.getColumnName(), outputCol, col.getType()));
            groupingColumnMapping.put(outputCol, col);
            outputIdx++;
        }

        // Aggregate result columns — names must match AggregateExpr output_name ("agg_0", "agg_1", ...)
        // which the Rust consumer also uses for Arrow field names
        for (int i = 0; i < aggregates.size(); i++) {
            AggregateFunction agg = aggregates.get(i);
            Type outputType = agg.getOutputType();
            String name = "agg_" + i;
            PrismColumnHandle outputCol = new PrismColumnHandle(name, outputIdx, outputType);
            projections.add(new Variable(name, outputType));
            outputAssignments.add(new Assignment(name, outputCol, outputType));
            outputIdx++;
        }

        return Optional.of(new AggregationApplicationResult<>(
                newHandle,
                projections,
                outputAssignments,
                groupingColumnMapping,
                false));
    }

    private String mapAggFunction(String trinoFuncName, boolean distinct) {
        if (distinct && "count".equalsIgnoreCase(trinoFuncName)) {
            return "COUNT_DISTINCT";
        }
        return switch (trinoFuncName.toLowerCase()) {
            case "count" -> "COUNT";
            case "sum" -> "SUM";
            case "min" -> "MIN";
            case "max" -> "MAX";
            case "avg" -> "AVG";
            default -> null;
        };
    }

    // ===== Expression Conversion =====

    private PrismPlanNode.ScalarExprNode convertCallExpression(Call call, Map<String, ColumnHandle> assignments) {
        String funcName = call.getFunctionName().getName();
        String op = switch (funcName) {
            case "$add" -> "ADD";
            case "$subtract" -> "SUBTRACT";
            case "$multiply" -> "MULTIPLY";
            case "$divide" -> "DIVIDE";
            case "$negate" -> "NEGATE";
            default -> null;
        };
        if (op == null) return null;

        List<PrismPlanNode.ScalarExprNode> args = new ArrayList<>();
        for (ConnectorExpression arg : call.getArguments()) {
            if (arg instanceof Variable var) {
                ColumnHandle ch = assignments.get(var.getName());
                if (ch instanceof PrismColumnHandle pch) {
                    args.add(new PrismPlanNode.ScalarExprNode.ColumnRef(pch.getColumnIndex()));
                } else {
                    return null;
                }
            } else if (arg instanceof Call nestedCall) {
                PrismPlanNode.ScalarExprNode nested = convertCallExpression(nestedCall, assignments);
                if (nested == null) return null;
                args.add(nested);
            } else if (arg instanceof Constant constant) {
                Object value = convertTrinoValue(constant.getType(), constant.getValue());
                String valueType = typeToValueType(constant.getType());
                args.add(new PrismPlanNode.ScalarExprNode.Literal(value, valueType));
            } else {
                return null;
            }
        }
        return new PrismPlanNode.ScalarExprNode.ArithmeticCall(op, args);
    }

    // ===== Helper =====

    PrismPlanNode buildFullScan(String tableName) {
        List<ColumnDef> defs = TPCH_TABLES.get(tableName);
        List<PrismPlanNode.ColumnRef> colRefs = new ArrayList<>();
        if (defs != null) {
            for (int i = 0; i < defs.size(); i++) {
                colRefs.add(new PrismPlanNode.ColumnRef(
                        defs.get(i).name(), i, defs.get(i).type().getDisplayName().toUpperCase()));
            }
        }
        return new PrismPlanNode.Scan(tableName, colRefs);
    }
}
