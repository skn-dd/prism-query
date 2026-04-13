package io.prism.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.prism.bridge.PrismFlightExecutor;
import io.prism.bridge.SubstraitSerializer;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

public class PrismPageSource implements ConnectorPageSource {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PrismFlightExecutor executor;
    private final PrismSplit split;
    private final PrismTableHandle tableHandle;
    private final List<PrismColumnHandle> columns;

    private boolean finished;
    private boolean executed;
    private long completedBytes;
    private long readTimeNanos;

    // Streaming state: holds the Flight stream open across getNextPage calls
    private FlightStream flightStream;
    private VectorSchemaRoot root;

    public PrismPageSource(
            PrismFlightExecutor executor,
            PrismSplit split,
            PrismTableHandle tableHandle,
            List<PrismColumnHandle> columns) {
        this.executor = executor;
        this.split = split;
        this.tableHandle = tableHandle;
        this.columns = columns;
    }

    @Override
    public long getCompletedBytes() { return completedBytes; }

    @Override
    public long getReadTimeNanos() { return readTimeNanos; }

    @Override
    public boolean isFinished() { return finished; }

    @Override
    public long getMemoryUsage() { return 0; }

    @Override
    public Page getNextPage() {
        if (finished) {
            return null;
        }

        long startNanos = System.nanoTime();
        try {
            if (!executed) {
                executeOnWorker();
                executed = true;
            }

            // Read next batch from Flight stream (zero-copy, no intermediate byte[])
            if (flightStream != null && flightStream.next()) {
                Page page = ArrowPageConverter.toPage(root, columns);
                readTimeNanos += System.nanoTime() - startNanos;
                return page;
            }

            // No more batches
            finished = true;
            readTimeNanos += System.nanoTime() - startNanos;
            return null;
        } catch (Exception e) {
            finished = true;
            throw new RuntimeException("Prism execution failed: " + e.getMessage(), e);
        }
    }

    private void executeOnWorker() throws Exception {
        // Build the plan to execute
        PrismPlanNode plan = tableHandle.getPushedPlan().orElseGet(() -> {
            // No pushed plan — build Scan with full table schema, then Project for requested columns
            var allDefs = PrismMetadata.getTableColumns(tableHandle.getTableName());
            List<PrismPlanNode.ColumnRef> allColRefs = new ArrayList<>();
            if (allDefs != null) {
                for (int i = 0; i < allDefs.size(); i++) {
                    var def = allDefs.get(i);
                    allColRefs.add(new PrismPlanNode.ColumnRef(def.name(), i, def.type().getDisplayName().toUpperCase()));
                }
            }
            PrismPlanNode scan = new PrismPlanNode.Scan(tableHandle.getTableName(), allColRefs);

            // If specific columns requested, wrap with Project
            if (!columns.isEmpty() && allDefs != null && columns.size() < allDefs.size()) {
                List<Integer> indices = columns.stream()
                        .map(PrismColumnHandle::getColumnIndex)
                        .toList();
                return new PrismPlanNode.Project(scan, indices);
            }
            return scan;
        });

        // Serialize to Substrait
        byte[] substraitBytes = SubstraitSerializer.serialize(plan);
        String planB64 = Base64.getEncoder().encodeToString(substraitBytes);

        // Build the execute command JSON
        String resultKey = "result/" + UUID.randomUUID();
        ObjectNode command = MAPPER.createObjectNode();
        command.put("substrait_plan_b64", planB64);
        command.put("result_key", resultKey);

        // Table-to-store-key mapping
        ObjectNode tablesNode = command.putObject("tables");
        tablesNode.put(tableHandle.getTableName(), "tpch/" + tableHandle.getTableName());
        addJoinTables(plan, tablesNode);

        // Execute on the worker
        executor.executeQuery(split.getWorkerIndex(), MAPPER.writeValueAsString(command));

        // Fetch results directly as Flight stream — no intermediate byte[] serialization
        flightStream = executor.fetchResultStream(split.getWorkerIndex(), resultKey);
        root = flightStream.getRoot();
    }

    @Override
    public void close() throws IOException {
        if (flightStream != null) {
            try {
                flightStream.close();
            } catch (Exception e) {
                throw new IOException("Failed to close FlightStream", e);
            }
        }
    }

    private void addJoinTables(PrismPlanNode node, ObjectNode tablesNode) {
        if (node instanceof PrismPlanNode.Join join) {
            addJoinTables(join.left(), tablesNode);
            addJoinTables(join.right(), tablesNode);
        } else if (node instanceof PrismPlanNode.Filter filter) {
            addJoinTables(filter.input(), tablesNode);
        } else if (node instanceof PrismPlanNode.Project project) {
            addJoinTables(project.input(), tablesNode);
        } else if (node instanceof PrismPlanNode.Aggregate agg) {
            addJoinTables(agg.input(), tablesNode);
        } else if (node instanceof PrismPlanNode.Sort sort) {
            addJoinTables(sort.input(), tablesNode);
        } else if (node instanceof PrismPlanNode.Scan scan) {
            tablesNode.put(scan.tableName(), "tpch/" + scan.tableName());
        }
    }
}
