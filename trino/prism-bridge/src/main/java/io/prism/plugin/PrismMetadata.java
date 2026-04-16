package io.prism.plugin;

import io.trino.spi.connector.*;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TopNApplicationResult;
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
import java.util.logging.Logger;

public class PrismMetadata implements ConnectorMetadata {
    private static final Logger LOG = Logger.getLogger(PrismMetadata.class.getName());

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
            new ColumnDef("l_linestatus", VarcharType.VARCHAR),
            new ColumnDef("l_shipdate", DateType.DATE),
            new ColumnDef("l_commitdate", DateType.DATE),
            new ColumnDef("l_receiptdate", DateType.DATE)
        ),
        "orders", List.of(
            new ColumnDef("o_orderkey", BigintType.BIGINT),
            new ColumnDef("o_custkey", BigintType.BIGINT),
            new ColumnDef("o_orderstatus", VarcharType.VARCHAR),
            new ColumnDef("o_totalprice", DoubleType.DOUBLE),
            new ColumnDef("o_orderpriority", VarcharType.VARCHAR),
            new ColumnDef("o_orderdate", DateType.DATE)
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

        // Try to extract expression-based predicates (LIKE, etc.)
        try {
            PrismPlanNode.PredicateNode exprPredicate = convertExpressionPredicate(
                    constraint.getExpression(), handle);
            if (exprPredicate != null) {
                predicate = (predicate == null) ? exprPredicate
                        : new PrismPlanNode.PredicateNode.And(predicate, exprPredicate);
            }
        } catch (Exception e) {
            // constraint.getExpression() may not be available; skip gracefully
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
        if (type instanceof DateType) {
            if (value instanceof Long) return ((Long) value).intValue();
            if (value instanceof Integer) return value;
            if (value instanceof Number) return ((Number) value).intValue();
            return value;
        }
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
        if (type instanceof DateType) return "DATE";
        if (type instanceof VarcharType) return "STRING";
        return "STRING";
    }

    private PrismPlanNode.PredicateNode convertExpressionPredicate(
            ConnectorExpression expr,
            PrismTableHandle handle) {
        if (expr instanceof Call call) {
            String funcName = call.getFunctionName().getName();
            List<ConnectorExpression> args = call.getArguments();

            // LIKE: $like(column, pattern)
            if ("$like".equals(funcName) && args.size() == 2) {
                if (args.get(0) instanceof Variable var && args.get(1) instanceof Constant pattern) {
                    int colIdx = findColumnIndex(handle.getTableName(), var.getName());
                    if (colIdx >= 0 && pattern.getValue() != null) {
                        String patternStr = convertTrinoValue(pattern.getType(), pattern.getValue()).toString();
                        return new PrismPlanNode.PredicateNode.Like(colIdx, patternStr, false);
                    }
                }
            }

            // AND of predicates
            if ("$and".equals(funcName) && args.size() == 2) {
                PrismPlanNode.PredicateNode left = convertExpressionPredicate(args.get(0), handle);
                PrismPlanNode.PredicateNode right = convertExpressionPredicate(args.get(1), handle);
                if (left != null && right != null) {
                    return new PrismPlanNode.PredicateNode.And(left, right);
                }
            }
        }
        return null; // Can't push this expression
    }

    private int findColumnIndex(String tableName, String columnName) {
        List<ColumnDef> defs = TPCH_TABLES.get(tableName);
        if (defs == null) return -1;
        for (int i = 0; i < defs.size(); i++) {
            if (defs.get(i).name().equals(columnName)) return i;
        }
        return -1;
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
        LOG.info("applyProjection called: table=" + handle.getTableName() +
                 ", projections=" + projections.size() +
                 ", assignments=" + assignments.keySet() +
                 ", hasPlan=" + handle.getPushedPlan().isPresent() +
                 ", planType=" + handle.getPushedPlan().map(p -> p.getClass().getSimpleName()).orElse("none"));

        List<ConnectorExpression> outputProjections = new ArrayList<>();
        Map<String, Assignment> outputAssignmentMap = new LinkedHashMap<>();
        List<PrismPlanNode.ScalarExprNode> computedExprs = new ArrayList<>();

        PrismPlanNode currentPlan = handle.getPushedPlan()
                .orElseGet(() -> buildFullScan(handle.getTableName()));
        int baseColumnCount = computePlanColumnCount(currentPlan, handle.getTableName());
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
            // Column-only projection: create a column-pruning Project if it reduces columns.
            // This absorbs Trino's ProjectNode so PushAggregationIntoTableScan can fire.
            // Convergence: only accept if selectCols.size() < currentOutputCols.
            if (handle.getPushedPlan().isPresent()) {
                List<Integer> selectCols = new ArrayList<>();
                Map<Integer, Integer> remap = new HashMap<>();
                for (ConnectorExpression proj : projections) {
                    if (proj instanceof Variable var) {
                        ColumnHandle ch = assignments.get(var.getName());
                        if (ch instanceof PrismColumnHandle pch) {
                            if (!remap.containsKey(pch.getColumnIndex())) {
                                remap.put(pch.getColumnIndex(), selectCols.size());
                                selectCols.add(pch.getColumnIndex());
                            }
                        }
                    }
                }
                int currentOutputCols = computePlanColumnCount(currentPlan, handle.getTableName());
                LOG.info("applyProjection: column-only pruning check: selectCols=" + selectCols.size() +
                         " vs currentOutputCols=" + currentOutputCols + ", selectCols=" + selectCols);
                if (selectCols.size() < currentOutputCols) {
                    PrismPlanNode projectPlan = new PrismPlanNode.Project(currentPlan, selectCols);
                    PrismTableHandle newHandle = handle.withPushedPlan(projectPlan);
                    // Remap output assignments to new 0-based positions.
                    // Deduplicate by new column index: if two variables reference the same
                    // column, keep one assignment and rewrite outputProjections to reference it.
                    Map<Integer, String> newIdxToVarName = new LinkedHashMap<>();
                    Map<String, Assignment> remapped = new LinkedHashMap<>();
                    for (var entry : outputAssignmentMap.entrySet()) {
                        Assignment a = entry.getValue();
                        PrismColumnHandle oldCol = (PrismColumnHandle) a.getColumn();
                        Integer newIdx = remap.get(oldCol.getColumnIndex());
                        if (newIdx != null) {
                            if (!newIdxToVarName.containsKey(newIdx)) {
                                newIdxToVarName.put(newIdx, a.getVariable());
                                PrismColumnHandle newCol = new PrismColumnHandle(oldCol.getColumnName(), newIdx, oldCol.getType());
                                remapped.put(entry.getKey(), new Assignment(a.getVariable(), newCol, a.getType()));
                            }
                        }
                    }
                    // Rewrite outputProjections: for each projection, use the canonical variable
                    // name for its column index
                    List<ConnectorExpression> rewrittenProjections = new ArrayList<>();
                    for (ConnectorExpression proj : projections) {
                        if (proj instanceof Variable var) {
                            ColumnHandle ch = assignments.get(var.getName());
                            if (ch instanceof PrismColumnHandle pch) {
                                Integer newIdx = remap.get(pch.getColumnIndex());
                                String canonicalName = newIdxToVarName.get(newIdx);
                                if (canonicalName != null) {
                                    rewrittenProjections.add(new Variable(canonicalName, var.getType()));
                                    continue;
                                }
                            }
                        }
                        rewrittenProjections.add(proj);
                    }
                    LOG.info("applyProjection: created column-pruning Project " + selectCols + " -> " + remapped.keySet());
                    return Optional.of(new ProjectionApplicationResult<>(
                            newHandle,
                            rewrittenProjections,
                            new ArrayList<>(remapped.values()),
                            false));
                }
                // selectCols >= currentOutputCols — no pruning possible, converge
                LOG.info("applyProjection: no pruning possible, returning empty to converge");
            }
            return Optional.empty();
        }

        // Build Project plan: passthrough all base columns + computed expressions
        List<Integer> passthrough = new ArrayList<>();
        for (int i = 0; i < baseColumnCount; i++) {
            passthrough.add(i);
        }
        PrismPlanNode projectPlan = new PrismPlanNode.Project(currentPlan, passthrough, computedExprs);
        PrismTableHandle newHandle = handle.withPushedPlan(projectPlan);

        // Include all base columns in output assignments (subsequent pushdowns need them)
        // Only when the plan is a bare Join or single-table Scan — NOT when it's already
        // a column-pruned Project (which would have wrong index mappings)
        if (currentPlan instanceof PrismPlanNode.Join joinNode) {
            String leftTable = findScanTableName(joinNode.left());
            String rightTable = findScanTableName(joinNode.right());
            if (leftTable != null) {
                for (int i = 0; i < getTableColumns(leftTable).size(); i++) {
                    ColumnDef def = getTableColumns(leftTable).get(i);
                    outputAssignmentMap.putIfAbsent(def.name(),
                            new Assignment(def.name(), new PrismColumnHandle(def.name(), i, def.type()), def.type()));
                }
            }
            if (rightTable != null) {
                int offset = leftTable != null ? getTableColumns(leftTable).size() : 0;
                for (int i = 0; i < getTableColumns(rightTable).size(); i++) {
                    ColumnDef def = getTableColumns(rightTable).get(i);
                    outputAssignmentMap.putIfAbsent(def.name(),
                            new Assignment(def.name(), new PrismColumnHandle(def.name(), offset + i, def.type()), def.type()));
                }
            }
        } else if (findJoinInPlan(currentPlan) == null) {
            // Single-table plan (no join underneath)
            List<ColumnDef> defs = getTableColumns(handle.getTableName());
            for (int i = 0; i < defs.size(); i++) {
                ColumnDef def = defs.get(i);
                outputAssignmentMap.putIfAbsent(def.name(),
                        new Assignment(def.name(), new PrismColumnHandle(def.name(), i, def.type()), def.type()));
            }
        }
        // For column-pruned plans over joins, skip — projected columns already in outputAssignmentMap

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
        LOG.info("applyAggregation called: table=" + handle.getTableName() +
                 ", aggregates=" + aggregates.size() +
                 ", assignments=" + assignments.keySet() +
                 ", groupingSets=" + groupingSets.size() +
                 ", hasPlan=" + handle.getPushedPlan().isPresent() +
                 ", planType=" + handle.getPushedPlan().map(p -> p.getClass().getSimpleName()).orElse("none"));

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

        // Resolve current plan and compute output column count
        PrismPlanNode currentPlan = handle.getPushedPlan()
                .orElseGet(() -> buildFullScan(handle.getTableName()));
        int baseColumnCount = computePlanColumnCount(currentPlan, handle.getTableName());
        // Column handles already have correct offset indices from applyJoin
        Map<String, ColumnHandle> effectiveAssignments = assignments;

        // Convert group-by to column indices (using remapped handles)
        List<Integer> groupByIndices = new ArrayList<>();
        for (ColumnHandle ch : groupByColumns) {
            PrismColumnHandle col = (PrismColumnHandle) ch;
            // Find remapped handle if available
            ColumnHandle remapped = effectiveAssignments.get(col.getColumnName());
            if (remapped instanceof PrismColumnHandle rch) {
                groupByIndices.add(rch.getColumnIndex());
            } else {
                groupByIndices.add(col.getColumnIndex());
            }
        }

        // Convert aggregates — handle both simple column refs and expression args
        List<PrismPlanNode.AggregateExpr> aggExprs = new ArrayList<>();
        List<PrismPlanNode.ScalarExprNode> computedExprs = new ArrayList<>();

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
                    ColumnHandle ch = effectiveAssignments.get(var.getName());
                    if (ch instanceof PrismColumnHandle pch) {
                        inputCol = pch.getColumnIndex();
                    }
                } else if (input instanceof Call call) {
                    PrismPlanNode.ScalarExprNode exprNode = convertCallExpression(call, effectiveAssignments);
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

        // Group-by columns come first in the output.
        // Output handles MUST differ from input handles (ImmutableBiMap in Trino's
        // PushAggregationIntoTableScan combines both in a BiMap requiring unique values).
        // Use "_gb_" prefix + output index to ensure distinctness.
        int outputIdx = 0;
        for (ColumnHandle ch : groupByColumns) {
            PrismColumnHandle col = (PrismColumnHandle) ch;
            String outputName = "_gb_" + col.getColumnName();
            PrismColumnHandle outputCol = new PrismColumnHandle(outputName, outputIdx, col.getType());
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

    // ===== TopN Pushdown =====

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments) {
        PrismTableHandle handle = (PrismTableHandle) table;

        LOG.info("applyTopN called: topNCount=" + topNCount +
                 ", sortItems=" + sortItems.size() +
                 ", assignments=" + assignments.keySet() +
                 ", table=" + handle.getTableName());

        if (topNCount <= 0 || topNCount > Integer.MAX_VALUE || sortItems.isEmpty()) {
            LOG.info("applyTopN: rejected (invalid count or empty sortItems)");
            return Optional.empty();
        }

        List<PrismPlanNode.SortKeyDef> sortKeys = new ArrayList<>();
        for (SortItem item : sortItems) {
            String colName = item.getName();
            ColumnHandle ch = assignments.get(colName);
            LOG.info("applyTopN: sortItem name='" + colName + "', found=" + (ch != null) +
                     ", isPrismCol=" + (ch instanceof PrismColumnHandle));
            if (!(ch instanceof PrismColumnHandle col)) {
                LOG.info("applyTopN: rejected (column '" + colName + "' not found or not PrismColumnHandle)");
                return Optional.empty();
            }
            String direction = item.getSortOrder().isAscending() ? "ASC" : "DESC";
            String nullOrdering = item.getSortOrder().isNullsFirst() ? "NULLS_FIRST" : "NULLS_LAST";
            sortKeys.add(new PrismPlanNode.SortKeyDef(col.getColumnIndex(), direction, nullOrdering));
        }

        PrismPlanNode currentPlan = handle.getPushedPlan()
                .orElseGet(() -> buildFullScan(handle.getTableName()));

        // Add column pruning: only read columns needed by output + sort keys
        // Collect all needed column indices (assignments = output, sortKeys = sort)
        Set<Integer> neededCols = new TreeSet<>();
        for (ColumnHandle ch : assignments.values()) {
            if (ch instanceof PrismColumnHandle col) {
                neededCols.add(col.getColumnIndex());
            }
        }
        for (PrismPlanNode.SortKeyDef key : sortKeys) {
            neededCols.add(key.columnIndex());
        }
        List<Integer> projCols = new ArrayList<>(neededCols);

        // Add Project to prune columns, then remap sort keys to projected positions
        PrismPlanNode projectPlan = new PrismPlanNode.Project(currentPlan, projCols);

        List<PrismPlanNode.SortKeyDef> remappedKeys = new ArrayList<>();
        for (PrismPlanNode.SortKeyDef key : sortKeys) {
            int newIdx = projCols.indexOf(key.columnIndex());
            remappedKeys.add(new PrismPlanNode.SortKeyDef(newIdx, key.direction(), key.nullOrdering()));
        }

        PrismPlanNode sortPlan = new PrismPlanNode.Sort(projectPlan, remappedKeys, topNCount);
        PrismTableHandle newHandle = handle.withPushedPlan(sortPlan);

        LOG.info("applyTopN: SUCCESS - pushed Sort with " + remappedKeys.size() + " keys, limit=" + topNCount +
                 ", projCols=" + projCols);
        return Optional.of(new TopNApplicationResult<>(newHandle, true, false));
    }

    // ===== Join Pushdown =====

    @Override
    public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(
            ConnectorSession session,
            JoinType joinType,
            ConnectorTableHandle left,
            ConnectorTableHandle right,
            ConnectorExpression joinCondition,
            Map<String, ColumnHandle> leftAssignments,
            Map<String, ColumnHandle> rightAssignments,
            JoinStatistics statistics) {
        PrismTableHandle leftHandle = (PrismTableHandle) left;
        PrismTableHandle rightHandle = (PrismTableHandle) right;

        LOG.info("applyJoin called: leftTable=" + leftHandle.getTableName() +
                 ", rightTable=" + rightHandle.getTableName() +
                 ", leftAssignments.keys=" + leftAssignments.keySet() +
                 ", rightAssignments.keys=" + rightAssignments.keySet());

        // Log exact handle details for debugging
        for (var entry : leftAssignments.entrySet()) {
            PrismColumnHandle ch = (PrismColumnHandle) entry.getValue();
            LOG.info("  leftAssignment: key='" + entry.getKey() + "' -> handle(name=" + ch.getColumnName() +
                     ", idx=" + ch.getColumnIndex() + ", type=" + ch.getType() +
                     ", class=" + ch.getClass().getName() + ", hash=" + ch.hashCode() + ")");
        }
        for (var entry : rightAssignments.entrySet()) {
            PrismColumnHandle ch = (PrismColumnHandle) entry.getValue();
            LOG.info("  rightAssignment: key='" + entry.getKey() + "' -> handle(name=" + ch.getColumnName() +
                     ", idx=" + ch.getColumnIndex() + ", type=" + ch.getType() +
                     ", class=" + ch.getClass().getName() + ", hash=" + ch.hashCode() + ")");
        }

        // Extract equi-join keys from the join condition
        List<Integer> leftKeys = new ArrayList<>();
        List<Integer> rightKeys = new ArrayList<>();

        if (!extractJoinKeys(joinCondition, leftAssignments, rightAssignments, leftKeys, rightKeys)) {
            return Optional.empty(); // Can't push non-equi joins
        }

        if (leftKeys.isEmpty()) {
            return Optional.empty(); // Need at least one equi-join key
        }

        // Map Trino JoinType to our internal string representation
        String prismJoinType = switch (joinType) {
            case INNER -> "INNER";
            case LEFT_OUTER -> "LEFT";
            case RIGHT_OUTER -> "RIGHT";
            case FULL_OUTER -> "FULL";
        };

        // Build the join plan node
        PrismPlanNode leftPlan = leftHandle.getPushedPlan()
                .orElseGet(() -> buildFullScan(leftHandle.getTableName()));
        PrismPlanNode rightPlan = rightHandle.getPushedPlan()
                .orElseGet(() -> buildFullScan(rightHandle.getTableName()));

        PrismPlanNode joinPlan = new PrismPlanNode.Join(leftPlan, rightPlan, prismJoinType, leftKeys, rightKeys);

        // Create a new table handle that carries the joined plan
        // Use left table as the "primary" table name
        PrismTableHandle joinHandle = new PrismTableHandle(
                leftHandle.getSchemaName(), leftHandle.getTableName())
                .withPushedPlan(joinPlan);

        // Build output column mappings. Trino validates that the mapping KEYS
        // match getColumnHandles(session, table).values() exactly — so keys must
        // use original table column indices, NOT sequential output indices.
        Map<ColumnHandle, ColumnHandle> leftColumnMapping = new LinkedHashMap<>();
        Map<ColumnHandle, ColumnHandle> rightColumnMapping = new LinkedHashMap<>();

        // Left side: identity mapping (old handle → old handle)
        List<ColumnDef> leftDefs = getTableColumns(leftHandle.getTableName());
        for (int i = 0; i < leftDefs.size(); i++) {
            ColumnDef def = leftDefs.get(i);
            PrismColumnHandle col = new PrismColumnHandle(def.name(), i, def.type());
            leftColumnMapping.put(col, col);
        }

        // Right side: old handle (key, idx 0..N) → new handle (value, idx leftCount+0..N)
        // Subsequent pushdowns see correct positions in join output
        List<ColumnDef> rightDefs = getTableColumns(rightHandle.getTableName());
        int leftColCount = leftDefs.size();
        for (int i = 0; i < rightDefs.size(); i++) {
            ColumnDef def = rightDefs.get(i);
            PrismColumnHandle oldCol = new PrismColumnHandle(def.name(), i, def.type());
            PrismColumnHandle newCol = new PrismColumnHandle(def.name(), leftColCount + i, def.type());
            rightColumnMapping.put(oldCol, newCol);
        }

        return Optional.of(new JoinApplicationResult<>(
                joinHandle,
                leftColumnMapping,
                rightColumnMapping,
                false));
    }

    private boolean extractJoinKeys(
            ConnectorExpression condition,
            Map<String, ColumnHandle> leftAssignments,
            Map<String, ColumnHandle> rightAssignments,
            List<Integer> leftKeys,
            List<Integer> rightKeys) {
        if (condition instanceof Call call) {
            String funcName = call.getFunctionName().getName();
            List<ConnectorExpression> args = call.getArguments();

            if ("$equal".equals(funcName) && args.size() == 2) {
                // Equi-join: left_col = right_col
                if (args.get(0) instanceof Variable leftVar && args.get(1) instanceof Variable rightVar) {
                    ColumnHandle leftCh = leftAssignments.get(leftVar.getName());
                    ColumnHandle rightCh = rightAssignments.get(rightVar.getName());
                    if (leftCh instanceof PrismColumnHandle lpc && rightCh instanceof PrismColumnHandle rpc) {
                        leftKeys.add(lpc.getColumnIndex());
                        rightKeys.add(rpc.getColumnIndex());
                        return true;
                    }
                    // Try reversed: right_col = left_col
                    leftCh = leftAssignments.get(rightVar.getName());
                    rightCh = rightAssignments.get(leftVar.getName());
                    if (leftCh instanceof PrismColumnHandle lpc && rightCh instanceof PrismColumnHandle rpc) {
                        leftKeys.add(lpc.getColumnIndex());
                        rightKeys.add(rpc.getColumnIndex());
                        return true;
                    }
                }
                return false;
            }

            if ("$and".equals(funcName) && args.size() == 2) {
                // AND chain of equi-join conditions
                boolean leftOk = extractJoinKeys(args.get(0), leftAssignments, rightAssignments, leftKeys, rightKeys);
                boolean rightOk = extractJoinKeys(args.get(1), leftAssignments, rightAssignments, leftKeys, rightKeys);
                return leftOk && rightOk;
            }
        }
        return false;
    }

    // ===== Limit Pushdown =====

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle table,
            long limit) {
        PrismTableHandle handle = (PrismTableHandle) table;

        if (limit <= 0 || limit > Integer.MAX_VALUE) {
            return Optional.empty();
        }

        PrismPlanNode currentPlan = handle.getPushedPlan()
                .orElseGet(() -> buildFullScan(handle.getTableName()));

        // Wrap in Sort with empty keys and limit (pure LIMIT, no ordering)
        PrismPlanNode limitPlan = new PrismPlanNode.Sort(currentPlan, List.of(), limit);
        PrismTableHandle newHandle = handle.withPushedPlan(limitPlan);

        return Optional.of(new LimitApplicationResult<>(newHandle, false, false));
    }

    // ===== Expression Conversion =====

    private PrismPlanNode.ScalarExprNode convertCallExpression(Call call, Map<String, ColumnHandle> assignments) {
        String funcName = call.getFunctionName().getName();

        // Handle CAST: unwrap and process the inner expression.
        // Rust evaluates all arithmetic as Float64, so explicit casts are no-ops.
        if ("$cast".equals(funcName)) {
            if (call.getArguments().isEmpty()) return null;
            ConnectorExpression inner = call.getArguments().get(0);
            if (inner instanceof Variable var) {
                ColumnHandle ch = assignments.get(var.getName());
                if (ch instanceof PrismColumnHandle pch) {
                    return new PrismPlanNode.ScalarExprNode.ColumnRef(pch.getColumnIndex());
                }
                return null;
            } else if (inner instanceof Call innerCall) {
                return convertCallExpression(innerCall, assignments);
            } else if (inner instanceof Constant constant) {
                Object value = convertTrinoValue(constant.getType(), constant.getValue());
                String valueType = typeToValueType(constant.getType());
                return new PrismPlanNode.ScalarExprNode.Literal(value, valueType);
            }
            return null;
        }

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

    /** Find a Join node in the plan tree (looking through Filter, Project, Aggregate, Sort). */
    private PrismPlanNode.Join findJoinInPlan(PrismPlanNode node) {
        if (node instanceof PrismPlanNode.Join join) return join;
        if (node instanceof PrismPlanNode.Filter f) return findJoinInPlan(f.input());
        if (node instanceof PrismPlanNode.Project p) return findJoinInPlan(p.input());
        if (node instanceof PrismPlanNode.Aggregate a) return findJoinInPlan(a.input());
        if (node instanceof PrismPlanNode.Sort s) return findJoinInPlan(s.input());
        return null;
    }

    /** Compute the output column count for a plan node. */
    private int computePlanColumnCount(PrismPlanNode plan, String tableName) {
        if (plan instanceof PrismPlanNode.Join join) {
            String leftTable = findScanTableName(join.left());
            String rightTable = findScanTableName(join.right());
            int leftCols = leftTable != null ? getTableColumns(leftTable).size() : 0;
            int rightCols = rightTable != null ? getTableColumns(rightTable).size() : 0;
            return leftCols + rightCols;
        }
        if (plan instanceof PrismPlanNode.Project project) {
            return project.columnIndices().size()
                    + (project.expressions() != null ? project.expressions().size() : 0);
        }
        if (plan instanceof PrismPlanNode.Aggregate agg) {
            return agg.groupBy().size() + agg.aggregates().size();
        }
        if (plan instanceof PrismPlanNode.Sort sort) {
            return computePlanColumnCount(sort.input(), tableName);
        }
        if (plan instanceof PrismPlanNode.Filter filter) {
            return computePlanColumnCount(filter.input(), tableName);
        }
        List<ColumnDef> defs = getTableColumns(tableName);
        return defs != null ? defs.size() : 0;
    }

    /** Walk a plan tree to find the table name from the nearest Scan node. */
    private String findScanTableName(PrismPlanNode node) {
        if (node instanceof PrismPlanNode.Scan scan) return scan.tableName();
        if (node instanceof PrismPlanNode.Filter f) return findScanTableName(f.input());
        if (node instanceof PrismPlanNode.Project p) return findScanTableName(p.input());
        if (node instanceof PrismPlanNode.Aggregate a) return findScanTableName(a.input());
        if (node instanceof PrismPlanNode.Sort s) return findScanTableName(s.input());
        return null;
    }

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
