package io.prism.plugin;

import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.BasicRelationStatistics;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.connector.SortItem;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Asserts that setting {@code acceleration_enabled=false} routes every pushdown
 * method to {@code Optional.empty()} (query goes through standard Trino execution)
 * while still allowing table resolution (no exception thrown).
 */
class PrismMetadataAccelerationDisabledTest {

    private final PrismMetadata metadata = new PrismMetadata();

    private TestSession disabledSession() {
        return TestSession.withDefaults()
                .with(PrismSessionProperties.ACCELERATION_ENABLED, false);
    }

    @Test
    void tableStillResolvesWhenAccelerationDisabled() {
        TestSession session = disabledSession();
        ConnectorTableHandle handle = metadata.getTableHandle(
                session,
                new SchemaTableName("tpch", "lineitem"),
                Optional.empty(),
                Optional.empty());
        assertTrue(handle instanceof PrismTableHandle,
                "table resolution must still work so SELECT ... FROM prism.tpch.lineitem runs");
    }

    @Test
    void applyFilterReturnsEmpty() {
        TestSession session = disabledSession();
        PrismTableHandle table = new PrismTableHandle("tpch", "lineitem");
        PrismColumnHandle col = new PrismColumnHandle("l_orderkey", 0, BigintType.BIGINT);
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(col, Domain.singleValue(BigintType.BIGINT, 1L)));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(session, table, new Constraint(domain));
        assertTrue(result.isEmpty(), "applyFilter must return empty when acceleration disabled");
    }

    @Test
    void applyAggregationReturnsEmpty() {
        TestSession session = disabledSession();
        PrismTableHandle table = new PrismTableHandle("tpch", "lineitem");
        AggregateFunction count = new AggregateFunction(
                "count", BigintType.BIGINT, List.of(), List.of(), false, Optional.empty());

        Optional<AggregationApplicationResult<ConnectorTableHandle>> result =
                metadata.applyAggregation(session, table, List.of(count), Map.of(), List.of(List.of()));
        assertTrue(result.isEmpty(), "applyAggregation must return empty when acceleration disabled");
    }

    @Test
    void applyProjectionReturnsEmpty() {
        TestSession session = disabledSession();
        PrismTableHandle table = new PrismTableHandle("tpch", "lineitem");
        Variable var = new Variable("l_orderkey", BigintType.BIGINT);

        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(
                        session,
                        table,
                        List.<ConnectorExpression>of(var),
                        Map.of("l_orderkey", new PrismColumnHandle("l_orderkey", 0, BigintType.BIGINT)));
        assertTrue(result.isEmpty(), "applyProjection must return empty when acceleration disabled");
    }

    @Test
    void applyTopNReturnsEmpty() {
        TestSession session = disabledSession();
        PrismTableHandle table = new PrismTableHandle("tpch", "lineitem");
        SortItem item = new SortItem("l_orderkey", SortOrder.ASC_NULLS_FIRST);

        Optional<TopNApplicationResult<ConnectorTableHandle>> result =
                metadata.applyTopN(
                        session,
                        table,
                        10,
                        List.of(item),
                        Map.of("l_orderkey", new PrismColumnHandle("l_orderkey", 0, BigintType.BIGINT)));
        assertTrue(result.isEmpty(), "applyTopN must return empty when acceleration disabled");
    }

    @Test
    void applyLimitReturnsEmpty() {
        TestSession session = disabledSession();
        PrismTableHandle table = new PrismTableHandle("tpch", "lineitem");

        Optional<LimitApplicationResult<ConnectorTableHandle>> result =
                metadata.applyLimit(session, table, 10);
        assertTrue(result.isEmpty(), "applyLimit must return empty when acceleration disabled");
    }

    @Test
    void applyJoinReturnsEmpty() {
        TestSession session = disabledSession();
        PrismTableHandle left = new PrismTableHandle("tpch", "lineitem");
        PrismTableHandle right = new PrismTableHandle("tpch", "orders");

        JoinStatistics emptyStats = new JoinStatistics() {
            @Override public Optional<BasicRelationStatistics> getLeftStatistics() { return Optional.empty(); }
            @Override public Optional<BasicRelationStatistics> getRightStatistics() { return Optional.empty(); }
            @Override public Optional<BasicRelationStatistics> getJoinStatistics() { return Optional.empty(); }
        };

        Optional<JoinApplicationResult<ConnectorTableHandle>> result =
                metadata.applyJoin(
                        session,
                        JoinType.INNER,
                        left,
                        right,
                        new Variable("dummy", BigintType.BIGINT),
                        Map.of(),
                        Map.of(),
                        emptyStats);
        assertTrue(result.isEmpty(), "applyJoin must return empty when acceleration disabled");
    }

    @Test
    void applyFilterStillFiresWhenAccelerationEnabled() {
        // Sanity: the disable flag gates pushdown without breaking the enabled path.
        TestSession session = TestSession.withDefaults();
        PrismTableHandle table = new PrismTableHandle("tpch", "lineitem");
        PrismColumnHandle col = new PrismColumnHandle("l_orderkey", 0, BigintType.BIGINT);
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(col, Domain.singleValue(BigintType.BIGINT, 1L)));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(session, table, new Constraint(domain));
        assertTrue(result.isPresent(), "applyFilter must still push down when acceleration enabled");
    }
}
