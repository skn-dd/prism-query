package io.prism.plugin;

import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrismMetadataProjectionTest {
    @Test
    void absorbsRenameOnlyProjectionOverJoinPlan() {
        PrismMetadata metadata = new PrismMetadata();
        PrismTableHandle handle = new PrismTableHandle("tpch", "lineitem").withPushedPlan(
                new PrismPlanNode.Join(
                        metadata.buildFullScan("lineitem"),
                        metadata.buildFullScan("orders"),
                        "INNER",
                        List.of(0),
                        List.of(0)));

        ProjectionInput input = buildRenameOnlyJoinProjection();

        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(null, handle, input.projections(), input.assignments());

        assertTrue(result.isPresent(), "rename-only projection over join should be absorbed");

        PrismTableHandle projectedHandle = (PrismTableHandle) result.get().getHandle();
        PrismPlanNode.Project projectedPlan = assertInstanceOf(
                PrismPlanNode.Project.class,
                projectedHandle.getPushedPlan().orElseThrow());
        assertEquals(input.totalColumns(), projectedPlan.columnIndices().size());
        assertTrue(projectedPlan.expressions().isEmpty());

        List<Assignment> outputAssignments = result.get().getAssignments();
        assertEquals(input.totalColumns(), outputAssignments.size());
    }

    @Test
    void convergesWhenSameColumnOnlyProjectionIsAlreadyPresent() {
        PrismMetadata metadata = new PrismMetadata();
        ProjectionInput input = buildRenameOnlyJoinProjection();

        List<Integer> allColumns = new ArrayList<>();
        for (int i = 0; i < input.totalColumns(); i++) {
            allColumns.add(i);
        }

        PrismTableHandle handle = new PrismTableHandle("tpch", "lineitem").withPushedPlan(
                new PrismPlanNode.Project(
                        new PrismPlanNode.Join(
                                metadata.buildFullScan("lineitem"),
                                metadata.buildFullScan("orders"),
                                "INNER",
                                List.of(0),
                                List.of(0)),
                        allColumns));

        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(null, handle, input.projections(), input.assignments());

        assertTrue(result.isEmpty(), "identical column-only projection should converge");
    }

    private ProjectionInput buildRenameOnlyJoinProjection() {
        List<ConnectorExpression> projections = new ArrayList<>();
        Map<String, ColumnHandle> assignments = new LinkedHashMap<>();
        int index = 0;

        for (PrismMetadata.ColumnDef def : PrismMetadata.getTableColumns("lineitem")) {
            String alias = "left_" + def.name();
            projections.add(new Variable(alias, def.type()));
            assignments.put(alias, new PrismColumnHandle(def.name(), index++, def.type()));
        }
        for (PrismMetadata.ColumnDef def : PrismMetadata.getTableColumns("orders")) {
            String alias = "right_" + def.name();
            projections.add(new Variable(alias, def.type()));
            assignments.put(alias, new PrismColumnHandle(def.name(), index++, def.type()));
        }

        return new ProjectionInput(projections, assignments, index);
    }

    private record ProjectionInput(
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments,
            int totalColumns) {}
}
