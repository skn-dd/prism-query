package io.prism.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.prism.bridge.PrismFlightExecutor;
import io.prism.bridge.SubstraitSerializer;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.*;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.airlift.slice.Slices;

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

    // Single-worker streaming state
    private FlightStream flightStream;
    private VectorSchemaRoot root;

    // Multi-worker merged result state
    private Page mergedPage;
    private boolean mergedPageReturned;

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
                int workerCount = executor.workerCount();
                if (workerCount > 1 && hasAggregation()) {
                    executeOnAllWorkers();
                } else {
                    executeOnWorker();
                }
                executed = true;
            }

            // Multi-worker path: return the single merged page
            if (mergedPage != null && !mergedPageReturned) {
                mergedPageReturned = true;
                readTimeNanos += System.nanoTime() - startNanos;
                Page page = mergedPage;
                finished = true;
                return page;
            }
            if (mergedPage != null) {
                finished = true;
                readTimeNanos += System.nanoTime() - startNanos;
                return null;
            }

            // Single-worker path: stream from Flight
            if (flightStream != null && flightStream.next()) {
                Page page = ArrowPageConverter.toPage(root, columns);
                readTimeNanos += System.nanoTime() - startNanos;
                return page;
            }

            finished = true;
            readTimeNanos += System.nanoTime() - startNanos;
            return null;
        } catch (Exception e) {
            finished = true;
            throw new RuntimeException("Prism execution failed: " + e.getMessage(), e);
        }
    }

    private boolean hasAggregation() {
        return tableHandle.getPushedPlan()
                .map(this::findAggregateNode)
                .isPresent();
    }

    private Optional<PrismPlanNode.Aggregate> findAggregateNode(PrismPlanNode node) {
        if (node instanceof PrismPlanNode.Aggregate agg) {
            return Optional.of(agg);
        }
        if (node instanceof PrismPlanNode.Sort sort) {
            return findAggregateNode(sort.input());
        }
        return Optional.empty();
    }

    /**
     * Fan-out: execute the same plan on all workers in parallel, merge partial aggregates.
     */
    private void executeOnAllWorkers() throws Exception {
        PrismPlanNode plan = tableHandle.getPushedPlan().orElseThrow();
        PrismPlanNode.Aggregate aggNode = findAggregateNode(plan).orElseThrow();

        byte[] substraitBytes = SubstraitSerializer.serialize(plan);
        String planB64 = Base64.getEncoder().encodeToString(substraitBytes);

        int workerCount = executor.workerCount();
        String[] resultKeys = new String[workerCount];

        // Build table mapping
        ObjectNode tablesTemplate = MAPPER.createObjectNode();
        tablesTemplate.put(tableHandle.getTableName(), "tpch/" + tableHandle.getTableName());
        addJoinTables(plan, tablesTemplate);

        // Execute on all workers in parallel
        CompletableFuture<?>[] futures = new CompletableFuture[workerCount];
        for (int i = 0; i < workerCount; i++) {
            final int wi = i;
            resultKeys[i] = "result/" + UUID.randomUUID();
            ObjectNode command = MAPPER.createObjectNode();
            command.put("substrait_plan_b64", planB64);
            command.put("result_key", resultKeys[i]);
            command.set("tables", tablesTemplate.deepCopy());
            String cmdJson = MAPPER.writeValueAsString(command);
            futures[i] = CompletableFuture.runAsync(() -> {
                try {
                    executor.executeQuery(wi, cmdJson);
                } catch (Exception e) {
                    throw new RuntimeException("Worker " + wi + " failed: " + e.getMessage(), e);
                }
            });
        }
        CompletableFuture.allOf(futures).join();

        // Fetch and merge results from all workers
        int groupByCount = aggNode.groupBy().size();
        List<PrismPlanNode.AggregateExpr> aggs = aggNode.aggregates();
        int aggCount = aggs.size();

        // group key → [agg accumulators], merge counts for AVG
        Map<List<Object>, double[]> merged = new LinkedHashMap<>();
        Map<List<Object>, int[]> avgCounts = new LinkedHashMap<>();

        for (int wi = 0; wi < workerCount; wi++) {
            try (FlightStream stream = executor.fetchResultStream(wi, resultKeys[wi])) {
                VectorSchemaRoot workerRoot = stream.getRoot();
                while (stream.next()) {
                    for (int row = 0; row < workerRoot.getRowCount(); row++) {
                        List<Object> groupKey = new ArrayList<>();
                        for (int g = 0; g < groupByCount; g++) {
                            groupKey.add(extractValue(workerRoot.getVector(g), row));
                        }

                        double[] accums = merged.computeIfAbsent(groupKey, k -> {
                            double[] a = new double[aggCount];
                            for (int ai = 0; ai < aggCount; ai++) {
                                if ("MIN".equals(aggs.get(ai).function())) a[ai] = Double.MAX_VALUE;
                                if ("MAX".equals(aggs.get(ai).function())) a[ai] = -Double.MAX_VALUE;
                            }
                            return a;
                        });
                        int[] cnts = avgCounts.computeIfAbsent(groupKey, k -> new int[aggCount]);

                        for (int ai = 0; ai < aggCount; ai++) {
                            double val = extractDouble(workerRoot.getVector(groupByCount + ai), row);
                            switch (aggs.get(ai).function()) {
                                case "SUM", "COUNT", "COUNT_DISTINCT" -> accums[ai] += val;
                                case "MIN" -> accums[ai] = Math.min(accums[ai], val);
                                case "MAX" -> accums[ai] = Math.max(accums[ai], val);
                                case "AVG" -> { accums[ai] += val; cnts[ai]++; }
                            }
                        }
                    }
                }
            }
        }

        // Finalize AVG: simple average of averages (valid for equal-size partitions)
        for (var entry : merged.entrySet()) {
            double[] accums = entry.getValue();
            int[] cnts = avgCounts.get(entry.getKey());
            for (int ai = 0; ai < aggCount; ai++) {
                if ("AVG".equals(aggs.get(ai).function()) && cnts[ai] > 0) {
                    accums[ai] /= cnts[ai];
                }
            }
        }

        // Build a Trino Page from merged results
        mergedPage = buildMergedPage(merged, groupByCount, aggs);
    }

    private Page buildMergedPage(Map<List<Object>, double[]> merged,
                                  int groupByCount,
                                  List<PrismPlanNode.AggregateExpr> aggs) {
        int rowCount = merged.size();
        if (rowCount == 0) {
            return new Page(0);
        }

        // If no columns requested (e.g., COUNT(*) with no output columns), return row count
        if (columns.isEmpty()) {
            return new Page(rowCount);
        }

        // Build blocks for each output column
        // Output order: group-by columns first, then aggregate columns
        int totalCols = groupByCount + aggs.size();
        BlockBuilder[] builders = new BlockBuilder[columns.size()];
        Type[] types = new Type[columns.size()];

        for (int c = 0; c < columns.size(); c++) {
            types[c] = columns.get(c).getType();
            builders[c] = types[c].createBlockBuilder(null, rowCount);
        }

        List<List<Object>> keys = new ArrayList<>(merged.keySet());
        List<double[]> vals = new ArrayList<>(merged.values());

        // The merged output has groupByCount + aggCount columns in order.
        // Map each requested column to the merged output by position.
        // Build a column index map: requested column name → position in merged output
        Map<String, Integer> mergedPositions = new HashMap<>();
        int pos = 0;
        for (int g = 0; g < groupByCount; g++) {
            // Group-by columns get their original names from the plan
            // But we can also just map by position
            mergedPositions.put("__group_" + g, pos++);
        }
        for (int a = 0; a < aggs.size(); a++) {
            mergedPositions.put(aggs.get(a).outputName(), pos++);
        }

        for (int row = 0; row < rowCount; row++) {
            for (int c = 0; c < columns.size(); c++) {
                // Use positional index: merged output is [group_by_0..N, agg_0..M]
                if (c < groupByCount) {
                    Object val = keys.get(row).get(c);
                    writeValue(builders[c], types[c], val);
                } else if (c - groupByCount < aggs.size()) {
                    double val = vals.get(row)[c - groupByCount];
                    writeDouble(builders[c], types[c], val);
                } else {
                    builders[c].appendNull();
                }
            }
        }

        var blocks = new io.trino.spi.block.Block[columns.size()];
        for (int c = 0; c < columns.size(); c++) {
            blocks[c] = builders[c].build();
        }
        return new Page(rowCount, blocks);
    }

    private void writeValue(BlockBuilder builder, Type type, Object val) {
        if (val == null) {
            builder.appendNull();
        } else if (type instanceof VarcharType) {
            type.writeSlice(builder, Slices.utf8Slice(val.toString()));
        } else if (type instanceof BigintType) {
            type.writeLong(builder, ((Number) val).longValue());
        } else if (type instanceof IntegerType) {
            type.writeLong(builder, ((Number) val).longValue());
        } else if (type instanceof DoubleType) {
            type.writeDouble(builder, ((Number) val).doubleValue());
        } else {
            type.writeSlice(builder, Slices.utf8Slice(val.toString()));
        }
    }

    private void writeDouble(BlockBuilder builder, Type type, double val) {
        if (type instanceof BigintType || type instanceof IntegerType) {
            type.writeLong(builder, (long) val);
        } else {
            type.writeDouble(builder, val);
        }
    }

    private Object extractValue(FieldVector vector, int row) {
        if (vector.isNull(row)) return null;
        if (vector instanceof BigIntVector v) return v.get(row);
        if (vector instanceof IntVector v) return v.get(row);
        if (vector instanceof Float8Vector v) return v.get(row);
        if (vector instanceof VarCharVector v) return new String(v.get(row), StandardCharsets.UTF_8);
        return vector.getObject(row).toString();
    }

    private double extractDouble(FieldVector vector, int row) {
        if (vector.isNull(row)) return 0.0;
        if (vector instanceof BigIntVector v) return (double) v.get(row);
        if (vector instanceof IntVector v) return (double) v.get(row);
        if (vector instanceof Float8Vector v) return v.get(row);
        if (vector instanceof Float4Vector v) return (double) v.get(row);
        return 0.0;
    }

    /**
     * Original single-worker execution path.
     */
    private void executeOnWorker() throws Exception {
        PrismPlanNode plan = tableHandle.getPushedPlan().orElseGet(() -> {
            var allDefs = PrismMetadata.getTableColumns(tableHandle.getTableName());
            List<PrismPlanNode.ColumnRef> allColRefs = new ArrayList<>();
            if (allDefs != null) {
                for (int i = 0; i < allDefs.size(); i++) {
                    var def = allDefs.get(i);
                    allColRefs.add(new PrismPlanNode.ColumnRef(def.name(), i, def.type().getDisplayName().toUpperCase()));
                }
            }
            PrismPlanNode scan = new PrismPlanNode.Scan(tableHandle.getTableName(), allColRefs);

            if (!columns.isEmpty() && allDefs != null && columns.size() < allDefs.size()) {
                List<Integer> indices = columns.stream()
                        .map(PrismColumnHandle::getColumnIndex)
                        .toList();
                return new PrismPlanNode.Project(scan, indices);
            }
            return scan;
        });

        byte[] substraitBytes = SubstraitSerializer.serialize(plan);
        String planB64 = Base64.getEncoder().encodeToString(substraitBytes);

        String resultKey = "result/" + UUID.randomUUID();
        ObjectNode command = MAPPER.createObjectNode();
        command.put("substrait_plan_b64", planB64);
        command.put("result_key", resultKey);

        ObjectNode tablesNode = command.putObject("tables");
        tablesNode.put(tableHandle.getTableName(), "tpch/" + tableHandle.getTableName());
        addJoinTables(plan, tablesNode);

        executor.executeQuery(split.getWorkerIndex(), MAPPER.writeValueAsString(command));

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
