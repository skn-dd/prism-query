package io.prism.bridge;

import io.prism.plugin.PrismPlanNode;
import io.prism.plugin.PrismPlanNode.*;
import io.substrait.proto.*;
import io.substrait.proto.Expression;
import io.substrait.proto.Expression.Literal;
import io.substrait.proto.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Serializes a PrismPlanNode tree into Substrait protobuf bytes
 * for execution by Prism Rust workers.
 *
 * Function reference IDs must match what prism-substrait/consumer.rs expects.
 */
public class SubstraitSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(SubstraitSerializer.class);

    // Scalar function reference IDs — must match consumer.rs
    public static final int FUNC_EQUAL = 1;
    public static final int FUNC_NOT_EQUAL = 2;
    public static final int FUNC_LESS_THAN = 3;
    public static final int FUNC_LESS_EQUAL = 4;
    public static final int FUNC_GREATER_THAN = 5;
    public static final int FUNC_GREATER_EQUAL = 6;
    public static final int FUNC_AND = 7;
    public static final int FUNC_OR = 8;
    public static final int FUNC_NOT = 9;

    // Arithmetic function reference IDs — must match consumer.rs
    public static final int FUNC_ADD = 10;
    public static final int FUNC_SUBTRACT = 11;
    public static final int FUNC_MULTIPLY = 12;
    public static final int FUNC_DIVIDE = 13;
    public static final int FUNC_NEGATE = 14;

    // String function reference IDs — must match consumer.rs
    public static final int FUNC_LIKE = 20;
    public static final int FUNC_ILIKE = 21;

    // Aggregate function reference IDs — must match consumer.rs
    public static final int AGG_COUNT = 0;
    public static final int AGG_SUM = 1;
    public static final int AGG_MIN = 2;
    public static final int AGG_MAX = 3;
    public static final int AGG_AVG = 4;
    public static final int AGG_COUNT_DISTINCT = 5;

    public static boolean isNativeSupported(String operatorType) {
        return switch (operatorType) {
            case "TableScanNode", "FilterNode", "ProjectNode",
                 "AggregationNode", "JoinNode", "SortNode",
                 "TopNNode", "ExchangeNode" -> true;
            default -> false;
        };
    }

    /**
     * Serialize a PrismPlanNode tree into Substrait protobuf bytes.
     * Applies optimizations (e.g., pushing projections into join scans) before serializing.
     */
    public static byte[] serialize(PrismPlanNode node) {
        PrismPlanNode optimized = pushProjectionsIntoJoin(node);
        Rel rootRel = serializeNode(optimized);

        Plan plan = Plan.newBuilder()
                .addRelations(PlanRel.newBuilder()
                        .setRoot(RelRoot.newBuilder()
                                .setInput(rootRel)
                                .build())
                        .build())
                .build();

        byte[] bytes = plan.toByteArray();
        LOG.debug("Serialized Substrait plan: {} bytes", bytes.length);
        return bytes;
    }

    /**
     * Push column projections through Join nodes into individual Scan nodes.
     * Transforms: Project([5,6,15], Join(Scan(L,all), Scan(R,all)))
     * Into:       Project([remapped], Join(Project([0,5,6], Scan(L)), Project([0,2], Scan(R))))
     *
     * Also handles nested Project chains: Project(A, Project(B, Join(...)))
     * by composing A and B into a single Project before pushing into the Join.
     *
     * This dramatically reduces I/O — only needed columns are read from Parquet.
     */
    private static PrismPlanNode pushProjectionsIntoJoin(PrismPlanNode node) {
        if (node instanceof PrismPlanNode.Project proj) {
            PrismPlanNode optimizedInput = pushProjectionsIntoJoin(proj.input());

            // Collapse Project → Project chains into a single Project when safe.
            // Trino can emit stacks of Projects (e.g., after multiple applyProjection calls).
            // We can only compose if the outer Project's output ordering matches the
            // "all passthroughs then all expressions" layout our executor uses; otherwise
            // composition reorders outputs and breaks downstream column references.
            if (optimizedInput instanceof PrismPlanNode.Project innerProj) {
                PrismPlanNode composed = composeProjects(proj, innerProj);
                if (composed != null) {
                    // Re-run to handle cases where the composed Project's input is a Join.
                    return pushProjectionsIntoJoin(composed);
                }
                // Composition would reorder outputs: keep nested structure
                if (optimizedInput != proj.input()) {
                    return new PrismPlanNode.Project(optimizedInput, proj.columnIndices(), proj.expressions());
                }
                return node;
            }

            if (optimizedInput instanceof PrismPlanNode.Join join) {
                return pushProjectIntoJoin(proj, join);
            }
            // Recurse but preserve the Project wrapping
            if (optimizedInput != proj.input()) {
                return new PrismPlanNode.Project(optimizedInput, proj.columnIndices(), proj.expressions());
            }
            return node;
        }
        if (node instanceof PrismPlanNode.Aggregate agg) {
            return new PrismPlanNode.Aggregate(pushProjectionsIntoJoin(agg.input()), agg.groupBy(), agg.aggregates());
        }
        if (node instanceof PrismPlanNode.Filter filter) {
            return new PrismPlanNode.Filter(pushProjectionsIntoJoin(filter.input()), filter.predicate());
        }
        if (node instanceof PrismPlanNode.Sort sort) {
            return new PrismPlanNode.Sort(pushProjectionsIntoJoin(sort.input()), sort.sortKeys(), sort.limit());
        }
        if (node instanceof PrismPlanNode.Join join) {
            return new PrismPlanNode.Join(
                    pushProjectionsIntoJoin(join.left()),
                    pushProjectionsIntoJoin(join.right()),
                    join.joinType(), join.leftKeys(), join.rightKeys());
        }
        return node; // Scan — leaf
    }

    /**
     * Given Project([cols], Join(left, right)), push column selections into each side.
     * Returns a new plan with Project nodes inside the Join for each side.
     */
    private static PrismPlanNode pushProjectIntoJoin(PrismPlanNode.Project proj, PrismPlanNode.Join join) {
        // Determine left/right OUTPUT column counts. When join.left()/.right() are
        // already Projects (from the Trino plan), their output width — not the
        // underlying Scan width — is what parent column indices reference.
        int leftColCount = countOutputColumns(join.left());
        int rightColCount = countOutputColumns(join.right());
        if (leftColCount <= 0 || rightColCount <= 0) {
            return proj; // Can't optimize without knowing schema sizes
        }

        // Collect all needed column indices from the Project + expressions
        java.util.Set<Integer> neededCols = new java.util.TreeSet<>();
        for (int col : proj.columnIndices()) {
            neededCols.add(col);
        }
        if (proj.expressions() != null) {
            for (PrismPlanNode.ScalarExprNode expr : proj.expressions()) {
                collectExprColumns(expr, neededCols);
            }
        }

        // Split into left-side and right-side columns
        java.util.Set<Integer> leftNeeded = new java.util.TreeSet<>();
        java.util.Set<Integer> rightNeeded = new java.util.TreeSet<>();
        for (int col : neededCols) {
            if (col < leftColCount) {
                leftNeeded.add(col);
            } else {
                rightNeeded.add(col - leftColCount);
            }
        }

        // Always include join keys
        if (join.leftKeys() != null) {
            for (int key : join.leftKeys()) leftNeeded.add(key);
        }
        if (join.rightKeys() != null) {
            for (int key : join.rightKeys()) rightNeeded.add(key);
        }

        // If both sides need all columns, nothing to optimize
        if (leftNeeded.size() >= leftColCount && rightNeeded.size() >= rightColCount) {
            return proj;
        }

        // Build left/right projection lists and column remaps
        List<Integer> leftProjCols = new ArrayList<>(leftNeeded);
        List<Integer> rightProjCols = new ArrayList<>(rightNeeded);
        java.util.Map<Integer, Integer> leftRemap = new java.util.HashMap<>();
        for (int i = 0; i < leftProjCols.size(); i++) {
            leftRemap.put(leftProjCols.get(i), i);
        }
        java.util.Map<Integer, Integer> rightRemap = new java.util.HashMap<>();
        for (int i = 0; i < rightProjCols.size(); i++) {
            rightRemap.put(rightProjCols.get(i), i);
        }

        // Remap join keys
        List<Integer> newLeftKeys = new ArrayList<>();
        for (int key : join.leftKeys()) {
            newLeftKeys.add(leftRemap.getOrDefault(key, key));
        }
        List<Integer> newRightKeys = new ArrayList<>();
        for (int key : join.rightKeys()) {
            newRightKeys.add(rightRemap.getOrDefault(key, key));
        }

        // Build inner Project nodes
        PrismPlanNode leftSide = leftProjCols.size() < leftColCount
                ? new PrismPlanNode.Project(join.left(), leftProjCols)
                : join.left();
        PrismPlanNode rightSide = rightProjCols.size() < rightColCount
                ? new PrismPlanNode.Project(join.right(), rightProjCols)
                : join.right();

        PrismPlanNode newJoin = new PrismPlanNode.Join(leftSide, rightSide, join.joinType(), newLeftKeys, newRightKeys);
        int newLeftCount = leftProjCols.size();

        // Remap outer Project column indices to the new join output positions
        List<Integer> newOuterCols = new ArrayList<>();
        for (int col : proj.columnIndices()) {
            if (col < leftColCount) {
                newOuterCols.add(leftRemap.getOrDefault(col, col));
            } else {
                int rightIdx = col - leftColCount;
                newOuterCols.add(newLeftCount + rightRemap.getOrDefault(rightIdx, rightIdx));
            }
        }

        // Remap expressions
        List<PrismPlanNode.ScalarExprNode> newExprs = null;
        if (proj.expressions() != null && !proj.expressions().isEmpty()) {
            newExprs = new ArrayList<>();
            for (PrismPlanNode.ScalarExprNode expr : proj.expressions()) {
                newExprs.add(remapExprColumns(expr, leftColCount, leftRemap, newLeftCount, rightRemap));
            }
        }

        LOG.info("Pushed projections into join: left {} cols -> {}, right {} cols -> {}", leftColCount, leftProjCols, rightColCount, rightProjCols);

        return new PrismPlanNode.Project(newJoin, newOuterCols, newExprs != null ? newExprs : List.of());
    }

    /**
     * Compose two nested Projects into a single Project.
     * Given outer = Project(A_cols, A_exprs) over inner = Project(B_cols, B_exprs, input),
     * produces a single Project(composed_cols, composed_exprs, input) that is equivalent
     * to applying inner then outer.
     */
    private static PrismPlanNode composeProjects(PrismPlanNode.Project outer, PrismPlanNode.Project inner) {
        int innerPassCount = inner.columnIndices().size();
        int innerExprCount = (inner.expressions() != null) ? inner.expressions().size() : 0;

        // Safety check: our Project executor always emits passthrough cols first, then
        // expression outputs. If outer's columnIndices interleaves refs to inner's cols
        // (<innerPassCount) and inner's exprs (>=innerPassCount) in a way that would change
        // output ordering after composition, skip composition and keep projects nested.
        boolean sawExprRef = false;
        for (int outerIdx : outer.columnIndices()) {
            boolean isExprRef = outerIdx >= innerPassCount;
            if (isExprRef) {
                sawExprRef = true;
            } else if (sawExprRef) {
                // A passthrough ref appears AFTER an expr ref: composition would reorder.
                LOG.info("composeProjects: skipping composition — outer cols {} would reorder after composing inner cols={} exprs={}",
                        outer.columnIndices(), innerPassCount, innerExprCount);
                return null;
            }
        }

        List<Integer> newCols = new ArrayList<>();
        List<PrismPlanNode.ScalarExprNode> newExprs = new ArrayList<>();

        // Resolve outer column references through inner
        for (int outerIdx : outer.columnIndices()) {
            if (outerIdx < innerPassCount) {
                // References a passthrough column in inner → resolve to input column
                newCols.add(inner.columnIndices().get(outerIdx));
            } else {
                // References an expression result from inner → pull the expression
                int exprIdx = outerIdx - innerPassCount;
                if (exprIdx < innerExprCount) {
                    newExprs.add(inner.expressions().get(exprIdx));
                }
            }
        }

        // Resolve outer expressions through inner
        if (outer.expressions() != null) {
            for (PrismPlanNode.ScalarExprNode outerExpr : outer.expressions()) {
                newExprs.add(resolveExprThroughProject(outerExpr, inner));
            }
        }

        LOG.info("composeProjects: outer cols={} exprs={}, inner cols={} exprs={} -> composed cols={} exprs={}",
                outer.columnIndices().size(),
                outer.expressions() != null ? outer.expressions().size() : 0,
                innerPassCount, innerExprCount,
                newCols.size(), newExprs.size());

        return new PrismPlanNode.Project(inner.input(), newCols, newExprs);
    }

    /**
     * Resolve column references in an expression through an inner Project.
     * Replaces ColumnRef indices that reference inner output positions with
     * the corresponding input column or expression.
     */
    private static PrismPlanNode.ScalarExprNode resolveExprThroughProject(
            PrismPlanNode.ScalarExprNode expr, PrismPlanNode.Project inner) {
        if (expr instanceof PrismPlanNode.ScalarExprNode.ColumnRef ref) {
            int idx = ref.columnIndex();
            int innerPassCount = inner.columnIndices().size();
            if (idx < innerPassCount) {
                // Maps to an input column through the inner passthrough
                return new PrismPlanNode.ScalarExprNode.ColumnRef(inner.columnIndices().get(idx));
            } else {
                // Maps to an inner expression result — inline the expression
                int exprIdx = idx - innerPassCount;
                if (inner.expressions() != null && exprIdx < inner.expressions().size()) {
                    return inner.expressions().get(exprIdx);
                }
            }
            return ref;
        }
        if (expr instanceof PrismPlanNode.ScalarExprNode.ArithmeticCall call) {
            List<PrismPlanNode.ScalarExprNode> newArgs = new ArrayList<>();
            for (PrismPlanNode.ScalarExprNode arg : call.args()) {
                newArgs.add(resolveExprThroughProject(arg, inner));
            }
            return new PrismPlanNode.ScalarExprNode.ArithmeticCall(call.op(), newArgs);
        }
        return expr; // Literal — no resolution needed
    }

    private static int countScanColumns(PrismPlanNode node) {
        if (node instanceof PrismPlanNode.Scan scan) {
            return scan.columns() != null ? scan.columns().size() : 0;
        }
        if (node instanceof PrismPlanNode.Filter f) return countScanColumns(f.input());
        if (node instanceof PrismPlanNode.Project p) return countScanColumns(p.input());
        return 0;
    }

    /**
     * Returns the number of OUTPUT columns produced by a node.
     * Project output = columnIndices + expressions. Scan output = all scan columns.
     */
    private static int countOutputColumns(PrismPlanNode node) {
        if (node instanceof PrismPlanNode.Scan scan) {
            return scan.columns() != null ? scan.columns().size() : 0;
        }
        if (node instanceof PrismPlanNode.Project p) {
            int cols = p.columnIndices() != null ? p.columnIndices().size() : 0;
            int exprs = p.expressions() != null ? p.expressions().size() : 0;
            return cols + exprs;
        }
        if (node instanceof PrismPlanNode.Filter f) return countOutputColumns(f.input());
        if (node instanceof PrismPlanNode.Sort s) return countOutputColumns(s.input());
        if (node instanceof PrismPlanNode.Join j) {
            return countOutputColumns(j.left()) + countOutputColumns(j.right());
        }
        return 0;
    }

    private static void collectExprColumns(PrismPlanNode.ScalarExprNode expr, java.util.Set<Integer> cols) {
        if (expr instanceof PrismPlanNode.ScalarExprNode.ColumnRef ref) {
            cols.add(ref.columnIndex());
        } else if (expr instanceof PrismPlanNode.ScalarExprNode.ArithmeticCall call) {
            for (PrismPlanNode.ScalarExprNode arg : call.args()) {
                collectExprColumns(arg, cols);
            }
        }
    }

    private static PrismPlanNode.ScalarExprNode remapExprColumns(
            PrismPlanNode.ScalarExprNode expr,
            int leftColCount,
            java.util.Map<Integer, Integer> leftRemap,
            int newLeftCount,
            java.util.Map<Integer, Integer> rightRemap) {
        if (expr instanceof PrismPlanNode.ScalarExprNode.ColumnRef ref) {
            int col = ref.columnIndex();
            if (col < leftColCount) {
                return new PrismPlanNode.ScalarExprNode.ColumnRef(leftRemap.getOrDefault(col, col));
            } else {
                int rightIdx = col - leftColCount;
                return new PrismPlanNode.ScalarExprNode.ColumnRef(newLeftCount + rightRemap.getOrDefault(rightIdx, rightIdx));
            }
        }
        if (expr instanceof PrismPlanNode.ScalarExprNode.ArithmeticCall call) {
            List<PrismPlanNode.ScalarExprNode> newArgs = new ArrayList<>();
            for (PrismPlanNode.ScalarExprNode arg : call.args()) {
                newArgs.add(remapExprColumns(arg, leftColCount, leftRemap, newLeftCount, rightRemap));
            }
            return new PrismPlanNode.ScalarExprNode.ArithmeticCall(call.op(), newArgs);
        }
        return expr; // Literal — no remapping
    }

    private static Rel serializeNode(PrismPlanNode node) {
        return switch (node) {
            case Scan scan -> serializeScan(scan);
            case Filter filter -> serializeFilter(filter);
            case Project project -> serializeProject(project);
            case Aggregate agg -> serializeAggregate(agg);
            case Join join -> serializeJoin(join);
            case Sort sort -> serializeSort(sort);
        };
    }

    // --- Scan ---

    private static Rel serializeScan(Scan scan) {
        ReadRel.Builder readBuilder = ReadRel.newBuilder()
                .setNamedTable(ReadRel.NamedTable.newBuilder()
                        .addNames(scan.tableName())
                        .build());

        // Build schema from column definitions
        if (scan.columns() != null && !scan.columns().isEmpty()) {
            NamedStruct.Builder namedStruct = NamedStruct.newBuilder();
            Type.Struct.Builder structBuilder = Type.Struct.newBuilder();

            for (ColumnRef col : scan.columns()) {
                namedStruct.addNames(col.name());
                structBuilder.addTypes(trinoTypeToSubstrait(col.type()));
            }
            namedStruct.setStruct(structBuilder.build());
            readBuilder.setBaseSchema(namedStruct.build());
        }

        return Rel.newBuilder()
                .setRead(readBuilder.build())
                .build();
    }

    // --- Filter ---

    private static Rel serializeFilter(Filter filter) {
        return Rel.newBuilder()
                .setFilter(FilterRel.newBuilder()
                        .setInput(serializeNode(filter.input()))
                        .setCondition(serializePredicate(filter.predicate()))
                        .build())
                .build();
    }

    // --- Project ---

    private static Rel serializeProject(Project project) {
        ProjectRel.Builder projBuilder = ProjectRel.newBuilder()
                .setInput(serializeNode(project.input()));

        for (int colIdx : project.columnIndices()) {
            projBuilder.addExpressions(makeColumnRef(colIdx));
        }

        // Computed expressions (arithmetic)
        if (project.expressions() != null) {
            for (PrismPlanNode.ScalarExprNode expr : project.expressions()) {
                projBuilder.addExpressions(serializeScalarExpr(expr));
            }
        }

        return Rel.newBuilder()
                .setProject(projBuilder.build())
                .build();
    }

    // --- Scalar expression serialization ---

    private static Expression serializeScalarExpr(PrismPlanNode.ScalarExprNode expr) {
        return switch (expr) {
            case PrismPlanNode.ScalarExprNode.ColumnRef ref -> makeColumnRef(ref.columnIndex());
            case PrismPlanNode.ScalarExprNode.Literal lit -> makeLiteral(lit.value(), lit.valueType());
            case PrismPlanNode.ScalarExprNode.ArithmeticCall call -> {
                int funcRef = arithmeticOpToRef(call.op());
                Expression.ScalarFunction.Builder funcBuilder = Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(funcRef);
                for (PrismPlanNode.ScalarExprNode arg : call.args()) {
                    funcBuilder.addArguments(FunctionArgument.newBuilder()
                            .setValue(serializeScalarExpr(arg))
                            .build());
                }
                yield Expression.newBuilder().setScalarFunction(funcBuilder.build()).build();
            }
        };
    }

    private static int arithmeticOpToRef(String op) {
        return switch (op.toUpperCase()) {
            case "ADD" -> FUNC_ADD;
            case "SUBTRACT" -> FUNC_SUBTRACT;
            case "MULTIPLY" -> FUNC_MULTIPLY;
            case "DIVIDE" -> FUNC_DIVIDE;
            case "NEGATE" -> FUNC_NEGATE;
            default -> throw new IllegalArgumentException("Unknown arithmetic op: " + op);
        };
    }

    // --- Aggregate ---

    private static Rel serializeAggregate(Aggregate agg) {
        AggregateRel.Builder aggBuilder = AggregateRel.newBuilder()
                .setInput(serializeNode(agg.input()));

        // Group-by columns
        if (agg.groupBy() != null && !agg.groupBy().isEmpty()) {
            AggregateRel.Grouping.Builder grouping = AggregateRel.Grouping.newBuilder();
            for (int colIdx : agg.groupBy()) {
                grouping.addGroupingExpressions(makeColumnRef(colIdx));
            }
            aggBuilder.addGroupings(grouping.build());
        }

        // Aggregate measures
        if (agg.aggregates() != null) {
            for (AggregateExpr aggExpr : agg.aggregates()) {
                int funcRef = aggFuncToRef(aggExpr.function());
                AggregateFunction.Builder funcBuilder = AggregateFunction.newBuilder()
                        .setFunctionReference(funcRef)
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(makeColumnRef(aggExpr.columnIndex()))
                                .build());

                aggBuilder.addMeasures(AggregateRel.Measure.newBuilder()
                        .setMeasure(funcBuilder.build())
                        .build());
            }
        }

        return Rel.newBuilder()
                .setAggregate(aggBuilder.build())
                .build();
    }

    // --- Join ---

    private static Rel serializeJoin(Join join) {
        JoinRel.Builder joinBuilder = JoinRel.newBuilder()
                .setLeft(serializeNode(join.left()))
                .setRight(serializeNode(join.right()))
                .setType(joinTypeToSubstrait(join.joinType()));

        // Build equi-join expression: left_key[0] = right_key[0] AND left_key[1] = right_key[1] ...
        if (join.leftKeys() != null && !join.leftKeys().isEmpty()) {
            Expression joinExpr = null;
            for (int i = 0; i < join.leftKeys().size(); i++) {
                Expression eq = makeEquality(join.leftKeys().get(i), join.rightKeys().get(i));
                if (joinExpr == null) {
                    joinExpr = eq;
                } else {
                    joinExpr = makeAnd(joinExpr, eq);
                }
            }
            joinBuilder.setExpression(joinExpr);
        }

        return Rel.newBuilder()
                .setJoin(joinBuilder.build())
                .build();
    }

    // --- Sort ---

    private static Rel serializeSort(Sort sort) {
        SortRel.Builder sortBuilder = SortRel.newBuilder()
                .setInput(serializeNode(sort.input()));

        if (sort.sortKeys() != null) {
            for (SortKeyDef key : sort.sortKeys()) {
                SortField.Builder sf = SortField.newBuilder()
                        .setExpr(makeColumnRef(key.columnIndex()))
                        .setDirection(SortField.SortDirection.forNumber(
                                sortDirToSubstrait(key.direction(), key.nullOrdering())));
                sortBuilder.addSorts(sf.build());
            }
        }

        // Handle LIMIT via FetchRel wrapper
        Rel sortRel = Rel.newBuilder().setSort(sortBuilder.build()).build();

        if (sort.limit() != null && sort.limit() > 0) {
            FetchRel fetch = FetchRel.newBuilder()
                    .setInput(sortRel)
                    .setOffset(0)
                    .setCount(sort.limit())
                    .build();
            return Rel.newBuilder().setFetch(fetch).build();
        }

        return sortRel;
    }

    // --- Predicate serialization ---

    private static Expression serializePredicate(PredicateNode pred) {
        return switch (pred) {
            case PredicateNode.Comparison comp -> serializeComparison(comp);
            case PredicateNode.And and -> makeAnd(
                    serializePredicate(and.left()),
                    serializePredicate(and.right()));
            case PredicateNode.Or or -> makeOr(
                    serializePredicate(or.left()),
                    serializePredicate(or.right()));
            case PredicateNode.Not not -> makeNot(serializePredicate(not.inner()));
            case PredicateNode.Like like -> serializeLike(like);
        };
    }

    private static Expression serializeComparison(PredicateNode.Comparison comp) {
        int funcRef = operatorToFuncRef(comp.operator());

        return Expression.newBuilder()
                .setScalarFunction(Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(funcRef)
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(makeColumnRef(comp.columnIndex()))
                                .build())
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(makeLiteral(comp.value(), comp.valueType()))
                                .build())
                        .build())
                .build();
    }

    private static Expression serializeLike(PredicateNode.Like like) {
        int funcRef = like.caseInsensitive() ? FUNC_ILIKE : FUNC_LIKE;
        return Expression.newBuilder()
                .setScalarFunction(Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(funcRef)
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(makeColumnRef(like.columnIndex()))
                                .build())
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(makeLiteral(like.pattern(), "STRING"))
                                .build())
                        .build())
                .build();
    }

    // --- Helper: column reference ---

    private static Expression makeColumnRef(int index) {
        return Expression.newBuilder()
                .setSelection(Expression.FieldReference.newBuilder()
                        .setDirectReference(Expression.ReferenceSegment.newBuilder()
                                .setStructField(Expression.ReferenceSegment.StructField.newBuilder()
                                        .setField(index)
                                        .build())
                                .build())
                        .build())
                .build();
    }

    // --- Helper: literals ---

    private static Expression makeLiteral(Object value, String valueType) {
        Literal.Builder lit = Literal.newBuilder();

        if (value == null) {
            lit.setNullable(true);
            return Expression.newBuilder().setLiteral(lit.build()).build();
        }

        switch (valueType != null ? valueType.toUpperCase() : "STRING") {
            case "BIGINT", "I64" -> lit.setI64(((Number) value).longValue());
            case "INTEGER", "I32" -> lit.setI32(((Number) value).intValue());
            case "DOUBLE", "FP64" -> lit.setFp64(((Number) value).doubleValue());
            case "BOOLEAN" -> lit.setBoolean((Boolean) value);
            case "DATE" -> lit.setDate(((Number) value).intValue());
            default -> lit.setString(value.toString());
        }

        return Expression.newBuilder().setLiteral(lit.build()).build();
    }

    // --- Helper: AND / OR / NOT ---

    private static Expression makeAnd(Expression left, Expression right) {
        return Expression.newBuilder()
                .setScalarFunction(Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(FUNC_AND)
                        .addArguments(FunctionArgument.newBuilder().setValue(left).build())
                        .addArguments(FunctionArgument.newBuilder().setValue(right).build())
                        .build())
                .build();
    }

    private static Expression makeOr(Expression left, Expression right) {
        return Expression.newBuilder()
                .setScalarFunction(Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(FUNC_OR)
                        .addArguments(FunctionArgument.newBuilder().setValue(left).build())
                        .addArguments(FunctionArgument.newBuilder().setValue(right).build())
                        .build())
                .build();
    }

    private static Expression makeNot(Expression inner) {
        return Expression.newBuilder()
                .setScalarFunction(Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(FUNC_NOT)
                        .addArguments(FunctionArgument.newBuilder().setValue(inner).build())
                        .build())
                .build();
    }

    private static Expression makeEquality(int leftCol, int rightCol) {
        return Expression.newBuilder()
                .setScalarFunction(Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(FUNC_EQUAL)
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(makeColumnRef(leftCol)).build())
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(makeColumnRef(rightCol)).build())
                        .build())
                .build();
    }

    // --- Type mapping ---

    private static Type trinoTypeToSubstrait(String typeName) {
        if (typeName == null) {
            return Type.newBuilder().setString(Type.String.newBuilder().build()).build();
        }
        return switch (typeName.toUpperCase()) {
            case "BIGINT" -> Type.newBuilder().setI64(Type.I64.newBuilder().build()).build();
            case "INTEGER" -> Type.newBuilder().setI32(Type.I32.newBuilder().build()).build();
            case "DOUBLE" -> Type.newBuilder().setFp64(Type.FP64.newBuilder().build()).build();
            case "REAL" -> Type.newBuilder().setFp32(Type.FP32.newBuilder().build()).build();
            case "BOOLEAN" -> Type.newBuilder().setBool(Type.Boolean.newBuilder().build()).build();
            case "DATE" -> Type.newBuilder().setDate(Type.Date.newBuilder().build()).build();
            default -> Type.newBuilder().setString(Type.String.newBuilder().build()).build();
        };
    }

    private static int operatorToFuncRef(String operator) {
        return switch (operator.toUpperCase()) {
            case "EQUAL", "=" -> FUNC_EQUAL;
            case "NOT_EQUAL", "!=" , "<>" -> FUNC_NOT_EQUAL;
            case "LESS_THAN", "<" -> FUNC_LESS_THAN;
            case "LESS_THAN_OR_EQUAL", "<=" -> FUNC_LESS_EQUAL;
            case "GREATER_THAN", ">" -> FUNC_GREATER_THAN;
            case "GREATER_THAN_OR_EQUAL", ">=" -> FUNC_GREATER_EQUAL;
            default -> throw new IllegalArgumentException("Unknown operator: " + operator);
        };
    }

    private static int aggFuncToRef(String func) {
        return switch (func.toUpperCase()) {
            case "COUNT" -> AGG_COUNT;
            case "SUM" -> AGG_SUM;
            case "MIN" -> AGG_MIN;
            case "MAX" -> AGG_MAX;
            case "AVG" -> AGG_AVG;
            case "COUNT_DISTINCT" -> AGG_COUNT_DISTINCT;
            default -> throw new IllegalArgumentException("Unknown aggregate: " + func);
        };
    }

    private static JoinRel.JoinType joinTypeToSubstrait(String joinType) {
        return switch (joinType.toUpperCase()) {
            case "INNER" -> JoinRel.JoinType.JOIN_TYPE_INNER;
            case "LEFT" -> JoinRel.JoinType.JOIN_TYPE_LEFT;
            case "RIGHT" -> JoinRel.JoinType.JOIN_TYPE_RIGHT;
            case "FULL" -> JoinRel.JoinType.JOIN_TYPE_OUTER;
            case "LEFT_SEMI" -> JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI;
            case "LEFT_ANTI" -> JoinRel.JoinType.JOIN_TYPE_LEFT_ANTI;
            default -> JoinRel.JoinType.JOIN_TYPE_INNER;
        };
    }

    private static int sortDirToSubstrait(String direction, String nullOrdering) {
        boolean desc = "DESC".equalsIgnoreCase(direction);
        boolean nullsFirst = "NULLS_FIRST".equalsIgnoreCase(nullOrdering);

        if (desc) {
            return nullsFirst
                    ? SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_FIRST_VALUE
                    : SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_LAST_VALUE;
        } else {
            return nullsFirst
                    ? SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_FIRST_VALUE
                    : SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST_VALUE;
        }
    }
}
