package io.prism.bridge;

import io.prism.plugin.PrismPlanNode;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies that unsupported-operator paths in the Substrait serializer
 * surface as {@code TrinoException(NOT_SUPPORTED)} rather than
 * un-classified {@code IllegalArgumentException} or {@code RuntimeException}.
 */
class SubstraitSerializerErrorTest {

    private static PrismPlanNode.Scan scan() {
        return new PrismPlanNode.Scan("t", List.of(
                new PrismPlanNode.ColumnRef("c0", 0, "BIGINT"),
                new PrismPlanNode.ColumnRef("c1", 1, "BIGINT")));
    }

    @Test
    void unsupportedComparisonOperatorMapsToNotSupported() {
        PrismPlanNode.PredicateNode bogusCmp = new PrismPlanNode.PredicateNode.Comparison(
                0, "BOGUS_OP", 1L, "BIGINT");
        PrismPlanNode.Filter plan = new PrismPlanNode.Filter(scan(), bogusCmp);

        TrinoException te = assertThrows(TrinoException.class,
                () -> SubstraitSerializer.serialize(plan));
        assertEquals(StandardErrorCode.NOT_SUPPORTED.toErrorCode(), te.getErrorCode());
    }

    @Test
    void unsupportedAggregateFunctionMapsToNotSupported() {
        PrismPlanNode.Aggregate agg = new PrismPlanNode.Aggregate(
                scan(),
                List.of(),
                List.of(new PrismPlanNode.AggregateExpr("STDDEV", 0, "agg_0")));
        TrinoException te = assertThrows(TrinoException.class,
                () -> SubstraitSerializer.serialize(agg));
        assertEquals(StandardErrorCode.NOT_SUPPORTED.toErrorCode(), te.getErrorCode());
    }

    @Test
    void unsupportedArithmeticOpMapsToNotSupported() {
        PrismPlanNode.ScalarExprNode bogus = new PrismPlanNode.ScalarExprNode.ArithmeticCall(
                "BOGUS",
                List.of(new PrismPlanNode.ScalarExprNode.ColumnRef(0),
                        new PrismPlanNode.ScalarExprNode.ColumnRef(1)));
        PrismPlanNode.Project plan = new PrismPlanNode.Project(
                scan(), List.of(0), List.of(bogus));
        TrinoException te = assertThrows(TrinoException.class,
                () -> SubstraitSerializer.serialize(plan));
        assertEquals(StandardErrorCode.NOT_SUPPORTED.toErrorCode(), te.getErrorCode());
    }
}
