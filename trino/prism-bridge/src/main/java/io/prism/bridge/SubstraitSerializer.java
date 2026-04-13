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
     */
    public static byte[] serialize(PrismPlanNode node) {
        Rel rootRel = serializeNode(node);

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
