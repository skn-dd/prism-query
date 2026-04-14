package io.prism.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

/**
 * Lightweight plan tree representation built up during Trino's pushdown.
 * Serialized to Substrait protobuf before sending to Prism workers.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = PrismPlanNode.Scan.class, name = "scan"),
    @JsonSubTypes.Type(value = PrismPlanNode.Filter.class, name = "filter"),
    @JsonSubTypes.Type(value = PrismPlanNode.Project.class, name = "project"),
    @JsonSubTypes.Type(value = PrismPlanNode.Aggregate.class, name = "aggregate"),
    @JsonSubTypes.Type(value = PrismPlanNode.Join.class, name = "join"),
    @JsonSubTypes.Type(value = PrismPlanNode.Sort.class, name = "sort"),
})
public sealed interface PrismPlanNode {

    record Scan(
        @JsonProperty("tableName") String tableName,
        @JsonProperty("columns") List<ColumnRef> columns
    ) implements PrismPlanNode {}

    record Filter(
        @JsonProperty("input") PrismPlanNode input,
        @JsonProperty("predicate") PredicateNode predicate
    ) implements PrismPlanNode {}

    record Project(
        @JsonProperty("input") PrismPlanNode input,
        @JsonProperty("columnIndices") List<Integer> columnIndices,
        @JsonProperty("expressions") List<ScalarExprNode> expressions
    ) implements PrismPlanNode {
        public Project(PrismPlanNode input, List<Integer> columnIndices) {
            this(input, columnIndices, List.of());
        }
    }

    record Aggregate(
        @JsonProperty("input") PrismPlanNode input,
        @JsonProperty("groupBy") List<Integer> groupBy,
        @JsonProperty("aggregates") List<AggregateExpr> aggregates
    ) implements PrismPlanNode {}

    record Join(
        @JsonProperty("left") PrismPlanNode left,
        @JsonProperty("right") PrismPlanNode right,
        @JsonProperty("joinType") String joinType,
        @JsonProperty("leftKeys") List<Integer> leftKeys,
        @JsonProperty("rightKeys") List<Integer> rightKeys
    ) implements PrismPlanNode {}

    record Sort(
        @JsonProperty("input") PrismPlanNode input,
        @JsonProperty("sortKeys") List<SortKeyDef> sortKeys,
        @JsonProperty("limit") Long limit
    ) implements PrismPlanNode {}

    // Supporting types

    record ColumnRef(
        @JsonProperty("name") String name,
        @JsonProperty("index") int index,
        @JsonProperty("type") String type
    ) {}

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = PredicateNode.Comparison.class, name = "comparison"),
        @JsonSubTypes.Type(value = PredicateNode.And.class, name = "and"),
        @JsonSubTypes.Type(value = PredicateNode.Or.class, name = "or"),
        @JsonSubTypes.Type(value = PredicateNode.Not.class, name = "not"),
        @JsonSubTypes.Type(value = PredicateNode.Like.class, name = "like"),
    })
    sealed interface PredicateNode {
        record Comparison(
            @JsonProperty("columnIndex") int columnIndex,
            @JsonProperty("operator") String operator,
            @JsonProperty("value") Object value,
            @JsonProperty("valueType") String valueType
        ) implements PredicateNode {}

        record And(
            @JsonProperty("left") PredicateNode left,
            @JsonProperty("right") PredicateNode right
        ) implements PredicateNode {}

        record Or(
            @JsonProperty("left") PredicateNode left,
            @JsonProperty("right") PredicateNode right
        ) implements PredicateNode {}

        record Not(
            @JsonProperty("inner") PredicateNode inner
        ) implements PredicateNode {}

        record Like(
            @JsonProperty("columnIndex") int columnIndex,
            @JsonProperty("pattern") String pattern,
            @JsonProperty("caseInsensitive") boolean caseInsensitive
        ) implements PredicateNode {}
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = ScalarExprNode.ColumnRef.class, name = "col_ref"),
        @JsonSubTypes.Type(value = ScalarExprNode.Literal.class, name = "literal"),
        @JsonSubTypes.Type(value = ScalarExprNode.ArithmeticCall.class, name = "arithmetic"),
    })
    sealed interface ScalarExprNode {
        record ColumnRef(
            @JsonProperty("columnIndex") int columnIndex
        ) implements ScalarExprNode {}

        record Literal(
            @JsonProperty("value") Object value,
            @JsonProperty("valueType") String valueType
        ) implements ScalarExprNode {}

        record ArithmeticCall(
            @JsonProperty("op") String op,
            @JsonProperty("args") List<ScalarExprNode> args
        ) implements ScalarExprNode {}
    }

    record AggregateExpr(
        @JsonProperty("function") String function,
        @JsonProperty("columnIndex") int columnIndex,
        @JsonProperty("outputName") String outputName
    ) {}

    record SortKeyDef(
        @JsonProperty("columnIndex") int columnIndex,
        @JsonProperty("direction") String direction,
        @JsonProperty("nullOrdering") String nullOrdering
    ) {}
}
