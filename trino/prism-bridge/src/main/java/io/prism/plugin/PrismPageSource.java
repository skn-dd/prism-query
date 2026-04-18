package io.prism.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.prism.bridge.PrismErrorMapper;
import io.prism.bridge.PrismFlightExecutor;
import io.prism.bridge.SubstraitSerializer;
import io.trino.spi.Page;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
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
import java.util.logging.Logger;

import io.airlift.slice.Slices;

public class PrismPageSource implements ConnectorPageSource {
    private static final Logger LOG = Logger.getLogger(PrismPageSource.class.getName());
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
                } else if (workerCount > 1 && hasSortWithLimit()) {
                    executeOnAllWorkersSort();
                } else if (workerCount > 1 && hasJoin()) {
                    executeOnAllWorkersConcat();
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
        } catch (TrinoException te) {
            finished = true;
            throw te;
        } catch (Exception e) {
            finished = true;
            // CompletableFuture.join() wraps causes in CompletionException; unwrap to
            // surface the classified TrinoException thrown by the worker lambda.
            Throwable root = e;
            while (root instanceof java.util.concurrent.CompletionException && root.getCause() != null) {
                root = root.getCause();
            }
            if (root instanceof TrinoException te) {
                throw te;
            }
            String workerAddr = executor.workerAddress(split.getWorkerIndex());
            throw PrismErrorMapper.wrap(workerAddr,
                    "page fetch for table " + tableHandle.getTableName(), root);
        }
    }

    private boolean hasAggregation() {
        return tableHandle.getPushedPlan()
                .flatMap(this::findAggregateNode)
                .isPresent();
    }

    private Optional<PrismPlanNode.Aggregate> findAggregateNode(PrismPlanNode node) {
        if (node instanceof PrismPlanNode.Aggregate agg) {
            return Optional.of(agg);
        }
        if (node instanceof PrismPlanNode.Sort sort) {
            return findAggregateNode(sort.input());
        }
        if (node instanceof PrismPlanNode.Project proj) {
            return findAggregateNode(proj.input());
        }
        return Optional.empty();
    }

    private boolean hasSortWithLimit() {
        return tableHandle.getPushedPlan()
                .flatMap(this::findSortNode)
                .isPresent();
    }

    private Optional<PrismPlanNode.Sort> findSortNode(PrismPlanNode node) {
        if (node instanceof PrismPlanNode.Sort sort && sort.limit() != null && sort.limit() > 0) {
            return Optional.of(sort);
        }
        return Optional.empty();
    }

    private boolean hasJoin() {
        return tableHandle.getPushedPlan()
                .map(this::findJoinNode)
                .orElse(false);
    }

    private boolean findJoinNode(PrismPlanNode node) {
        if (node instanceof PrismPlanNode.Join) return true;
        if (node instanceof PrismPlanNode.Filter f) return findJoinNode(f.input());
        if (node instanceof PrismPlanNode.Project p) return findJoinNode(p.input());
        if (node instanceof PrismPlanNode.Aggregate a) return findJoinNode(a.input());
        if (node instanceof PrismPlanNode.Sort s) return findJoinNode(s.input());
        return false;
    }

    /**
     * Fan-out: execute join plan on all workers, concatenate results.
     * Each worker joins its local lineitem/orders partitions.
     * Trino handles subsequent aggregation.
     */
    private void executeOnAllWorkersConcat() throws Exception {
        PrismPlanNode plan = tableHandle.getPushedPlan().orElseThrow();
        byte[] substraitBytes = SubstraitSerializer.serialize(plan);
        String planB64 = Base64.getEncoder().encodeToString(substraitBytes);

        int workerCount = executor.workerCount();
        String[] resultKeys = new String[workerCount];

        ObjectNode tablesTemplate = MAPPER.createObjectNode();
        putTableSpec(tablesTemplate, tableHandle.getTableName(), "tpch/" + tableHandle.getTableName());
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
                } catch (TrinoException te) {
                    throw te;
                } catch (Exception e) {
                    throw PrismErrorMapper.wrap(executor.workerAddress(wi),
                            "executeQuery (worker " + wi + ")", e);
                }
            });
        }
        CompletableFuture.allOf(futures).join();

        // Collect all rows from all workers
        List<Object[]> allRows = new ArrayList<>();
        for (int wi = 0; wi < workerCount; wi++) {
            try (FlightStream stream = executor.fetchResultStream(wi, resultKeys[wi])) {
                VectorSchemaRoot workerRoot = stream.getRoot();
                while (stream.next()) {
                    int vecCount = workerRoot.getFieldVectors().size();
                    for (int row = 0; row < workerRoot.getRowCount(); row++) {
                        Object[] values = new Object[columns.size()];
                        for (int c = 0; c < columns.size(); c++) {
                            if (c < vecCount) {
                                values[c] = extractValue(workerRoot.getVector(c), row);
                            }
                        }
                        allRows.add(values);
                    }
                }
            }
        }

        mergedPage = buildSortedPage(allRows);
    }

    private List<Integer> findProjectColumns(PrismPlanNode node) {
        if (node instanceof PrismPlanNode.Sort sort) {
            return findProjectColumns(sort.input());
        }
        if (node instanceof PrismPlanNode.Project proj) {
            return proj.columnIndices();
        }
        return null;
    }

    /**
     * Fan-out: execute the same plan on all workers in parallel, merge partial aggregates.
     */
    private void executeOnAllWorkers() throws Exception {
        PrismPlanNode plan = tableHandle.getPushedPlan().orElseThrow();
        PrismPlanNode.Aggregate aggNode = findAggregateNode(plan).orElseThrow();
        List<PrismPlanNode.AggregateExpr> originalAggs = aggNode.aggregates();
        int groupByCount = aggNode.groupBy().size();

        // Expand AVG into SUM + COUNT for correct multi-worker merge
        List<PrismPlanNode.AggregateExpr> workerAggs = new ArrayList<>();
        // Maps original agg index -> [workerAgg indices]. For AVG, maps to [sumIdx, countIdx].
        List<int[]> aggMapping = new ArrayList<>();

        for (int i = 0; i < originalAggs.size(); i++) {
            PrismPlanNode.AggregateExpr agg = originalAggs.get(i);
            if ("AVG".equals(agg.function())) {
                int sumIdx = workerAggs.size();
                workerAggs.add(new PrismPlanNode.AggregateExpr("SUM", agg.columnIndex(), "agg_" + workerAggs.size()));
                int countIdx = workerAggs.size();
                workerAggs.add(new PrismPlanNode.AggregateExpr("COUNT", agg.columnIndex(), "agg_" + workerAggs.size()));
                aggMapping.add(new int[]{sumIdx, countIdx});
            } else {
                int idx = workerAggs.size();
                workerAggs.add(new PrismPlanNode.AggregateExpr(agg.function(), agg.columnIndex(), "agg_" + workerAggs.size()));
                aggMapping.add(new int[]{idx});
            }
        }

        // Build modified plan with expanded aggregates for workers
        PrismPlanNode workerPlan = replaceAggregates(plan, aggNode, workerAggs);

        byte[] substraitBytes = SubstraitSerializer.serialize(workerPlan);
        String planB64 = Base64.getEncoder().encodeToString(substraitBytes);

        int workerCount = executor.workerCount();
        String[] resultKeys = new String[workerCount];

        // Build table mapping
        ObjectNode tablesTemplate = MAPPER.createObjectNode();
        putTableSpec(tablesTemplate, tableHandle.getTableName(), "tpch/" + tableHandle.getTableName());
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
                } catch (TrinoException te) {
                    throw te;
                } catch (Exception e) {
                    throw PrismErrorMapper.wrap(executor.workerAddress(wi),
                            "executeQuery (worker " + wi + ")", e);
                }
            });
        }
        CompletableFuture.allOf(futures).join();

        // Merge using EXPANDED aggregates
        int workerAggCount = workerAggs.size();
        Map<List<Object>, double[]> merged = new LinkedHashMap<>();

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
                            double[] a = new double[workerAggCount];
                            for (int ai = 0; ai < workerAggCount; ai++) {
                                if ("MIN".equals(workerAggs.get(ai).function())) a[ai] = Double.MAX_VALUE;
                                if ("MAX".equals(workerAggs.get(ai).function())) a[ai] = -Double.MAX_VALUE;
                            }
                            return a;
                        });

                        for (int ai = 0; ai < workerAggCount; ai++) {
                            double val = extractDouble(workerRoot.getVector(groupByCount + ai), row);
                            switch (workerAggs.get(ai).function()) {
                                case "SUM", "COUNT", "COUNT_DISTINCT" -> accums[ai] += val;
                                case "MIN" -> accums[ai] = Math.min(accums[ai], val);
                                case "MAX" -> accums[ai] = Math.max(accums[ai], val);
                            }
                        }
                    }
                }
            }
        }

        // Reconstruct original aggregate results from expanded results
        Map<List<Object>, double[]> finalResults = new LinkedHashMap<>();
        for (var entry : merged.entrySet()) {
            double[] workerAccums = entry.getValue();
            double[] finalAccums = new double[originalAggs.size()];
            for (int i = 0; i < originalAggs.size(); i++) {
                int[] mapping = aggMapping.get(i);
                if ("AVG".equals(originalAggs.get(i).function())) {
                    // AVG = SUM / COUNT
                    double sum = workerAccums[mapping[0]];
                    double count = workerAccums[mapping[1]];
                    finalAccums[i] = count > 0 ? sum / count : 0.0;
                } else {
                    finalAccums[i] = workerAccums[mapping[0]];
                }
            }
            finalResults.put(entry.getKey(), finalAccums);
        }

        // Build a Trino Page from merged results
        mergedPage = buildMergedPage(finalResults, groupByCount, originalAggs);
    }

    /**
     * Fan-out: execute Sort/TopN plan on all workers, merge-sort results, apply final limit.
     */
    private void executeOnAllWorkersSort() throws Exception {
        PrismPlanNode plan = tableHandle.getPushedPlan().orElseThrow();
        PrismPlanNode.Sort sortNode = findSortNode(plan).orElseThrow();
        long limit = sortNode.limit();

        byte[] substraitBytes = SubstraitSerializer.serialize(plan);
        String planB64 = Base64.getEncoder().encodeToString(substraitBytes);

        int workerCount = executor.workerCount();
        String[] resultKeys = new String[workerCount];

        ObjectNode tablesTemplate = MAPPER.createObjectNode();
        putTableSpec(tablesTemplate, tableHandle.getTableName(), "tpch/" + tableHandle.getTableName());
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
                } catch (TrinoException te) {
                    throw te;
                } catch (Exception e) {
                    throw PrismErrorMapper.wrap(executor.workerAddress(wi),
                            "executeQuery (worker " + wi + ")", e);
                }
            });
        }
        CompletableFuture.allOf(futures).join();

        // The plan is Sort(Project([orig_col_indices...]), ...) — worker returns
        // only the projected columns. Build mapping from original table index to
        // projected vector position.
        List<PrismPlanNode.SortKeyDef> sortKeys = sortNode.sortKeys();

        List<Integer> projCols = findProjectColumns(plan);
        Map<Integer, Integer> origToProj = new HashMap<>();
        if (projCols != null) {
            for (int p = 0; p < projCols.size(); p++) {
                origToProj.put(projCols.get(p), p);
            }
        }

        // Collect rows from all workers
        List<Object[]> flatRows = new ArrayList<>();
        for (int wi = 0; wi < workerCount; wi++) {
            try (FlightStream stream = executor.fetchResultStream(wi, resultKeys[wi])) {
                VectorSchemaRoot workerRoot = stream.getRoot();
                while (stream.next()) {
                    int vecCount = workerRoot.getFieldVectors().size();
                    for (int row = 0; row < workerRoot.getRowCount(); row++) {
                        Object[] values = new Object[columns.size()];
                        for (int c = 0; c < columns.size(); c++) {
                            int origIdx = columns.get(c).getColumnIndex();
                            int vecIdx = projCols != null
                                    ? origToProj.getOrDefault(origIdx, -1)
                                    : origIdx;
                            if (vecIdx >= 0 && vecIdx < vecCount) {
                                values[c] = extractValue(workerRoot.getVector(vecIdx), row);
                            }
                        }
                        flatRows.add(values);
                    }
                }
            }
        }

        // Map sort key (in projected space) to slot in row array
        // Sort keys reference projected positions; find which output column that maps to
        Map<Integer, Integer> projPosToSlot = new HashMap<>();
        for (int c = 0; c < columns.size(); c++) {
            int origIdx = columns.get(c).getColumnIndex();
            int projIdx = projCols != null
                    ? origToProj.getOrDefault(origIdx, origIdx)
                    : origIdx;
            projPosToSlot.put(projIdx, c);
        }

        // Merge-sort by sort keys, then apply limit
        flatRows.sort((a, b) -> {
            for (PrismPlanNode.SortKeyDef key : sortKeys) {
                Integer slot = projPosToSlot.get(key.columnIndex());
                if (slot == null) continue;

                @SuppressWarnings("unchecked")
                Comparable<Object> va = (Comparable<Object>) a[slot];
                @SuppressWarnings("unchecked")
                Comparable<Object> vb = (Comparable<Object>) b[slot];
                if (va == null && vb == null) continue;
                if (va == null) return "ASC".equals(key.direction()) ? -1 : 1;
                if (vb == null) return "ASC".equals(key.direction()) ? 1 : -1;

                int cmp = va.compareTo(vb);
                if ("DESC".equals(key.direction())) cmp = -cmp;
                if (cmp != 0) return cmp;
            }
            return 0;
        });

        int finalLimit = (int) Math.min(limit, flatRows.size());
        List<Object[]> topN = flatRows.subList(0, finalLimit);

        mergedPage = buildSortedPage(topN);
    }

    private Page buildSortedPage(List<Object[]> rows) {
        int rowCount = rows.size();
        if (rowCount == 0 || columns.isEmpty()) {
            return new Page(rowCount);
        }

        BlockBuilder[] builders = new BlockBuilder[columns.size()];
        Type[] types = new Type[columns.size()];
        for (int c = 0; c < columns.size(); c++) {
            types[c] = columns.get(c).getType();
            builders[c] = types[c].createBlockBuilder(null, rowCount);
        }

        for (Object[] row : rows) {
            for (int c = 0; c < columns.size(); c++) {
                writeValue(builders[c], types[c], row[c]);
            }
        }

        var blocks = new io.trino.spi.block.Block[columns.size()];
        for (int c = 0; c < columns.size(); c++) {
            blocks[c] = builders[c].build();
        }
        return new Page(rowCount, blocks);
    }

    private PrismPlanNode replaceAggregates(PrismPlanNode plan, PrismPlanNode.Aggregate originalAgg, List<PrismPlanNode.AggregateExpr> newAggs) {
        if (plan instanceof PrismPlanNode.Aggregate) {
            return new PrismPlanNode.Aggregate(originalAgg.input(), originalAgg.groupBy(), newAggs);
        }
        if (plan instanceof PrismPlanNode.Sort sort) {
            return new PrismPlanNode.Sort(replaceAggregates(sort.input(), originalAgg, newAggs), sort.sortKeys(), sort.limit());
        }
        if (plan instanceof PrismPlanNode.Project proj) {
            return new PrismPlanNode.Project(replaceAggregates(proj.input(), originalAgg, newAggs), proj.columnIndices(), proj.expressions());
        }
        return plan;
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
        putTableSpec(tablesNode, tableHandle.getTableName(), "tpch/" + tableHandle.getTableName());
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
        addJoinTablesImpl(node, tablesNode, false);
    }

    private void addJoinTablesImpl(PrismPlanNode node, ObjectNode tablesNode, boolean broadcast) {
        if (node instanceof PrismPlanNode.Join join) {
            addJoinTablesImpl(join.left(), tablesNode, false);
            // Right side of join uses broadcast (all workers read full table)
            addJoinTablesImpl(join.right(), tablesNode, true);
        } else if (node instanceof PrismPlanNode.Filter filter) {
            addJoinTablesImpl(filter.input(), tablesNode, broadcast);
        } else if (node instanceof PrismPlanNode.Project project) {
            addJoinTablesImpl(project.input(), tablesNode, broadcast);
        } else if (node instanceof PrismPlanNode.Aggregate agg) {
            addJoinTablesImpl(agg.input(), tablesNode, broadcast);
        } else if (node instanceof PrismPlanNode.Sort sort) {
            addJoinTablesImpl(sort.input(), tablesNode, broadcast);
        } else if (node instanceof PrismPlanNode.Scan scan) {
            String storeKey = broadcast
                    ? "tpch/" + scan.tableName() + "_broadcast"
                    : "tpch/" + scan.tableName();
            putTableSpec(tablesNode, scan.tableName(), storeKey);
        }
    }

    /**
     * Write a v2 table spec entry into the `tables` JSON map.
     *
     * Protocol v2 (Wave 2a):
     * {@code "<name>": { "uris": [...], "store_key": "<legacy>" } }
     *
     * For the TPC-H bench path, {@code uris} is empty and the worker resolves
     * the table via {@code --data-dir/<store_key>/} (legacy Parquet on disk)
     * or the in-memory PartitionStore. Once metadata delegation lands in
     * Wave 2b, {@code uris} will be populated with fully-qualified file URIs
     * (s3://, file://, etc.) and {@code store_key} will be omitted.
     *
     * The JSON-object shape leaves room for future fields (schema, format,
     * ...) without another breaking protocol change.
     */
    // package-private for unit testing
    static void putTableSpec(ObjectNode tablesNode, String tableName, String storeKey) {
        ObjectNode spec = tablesNode.putObject(tableName);
        spec.putArray("uris");
        spec.put("store_key", storeKey);
    }
}
