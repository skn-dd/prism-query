package io.prism.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.prism.plugin.PrismPlanNode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.*;

/**
 * Benchmark coordinator — generates TPC-H data, distributes to Rust workers,
 * executes queries via Arrow Flight, and reports results.
 *
 * <p>This simulates the Trino coordinator's role: data generation/partitioning,
 * plan dispatch, and result collection. In production, Trino's SQL parser and
 * optimizer would produce these plans; here we construct them directly.</p>
 *
 * <p>Usage: java -jar prism-bridge.jar [--workers host:port,host:port] [--sf 0.1]</p>
 */
public class BenchmarkCoordinator {
    private static final int BENCH_CHUNK_ROWS = 1_000_000;

    private static final BufferAllocator ALLOC = new RootAllocator(Long.MAX_VALUE);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // Parse arguments
        List<String> workers = List.of("localhost:50051", "localhost:50052");
        double sf = 0.1;
        String bench = "all";
        boolean skipLoad = false;

        for (int i = 0; i < args.length; i++) {
            if ("--workers".equals(args[i]) && i + 1 < args.length) {
                workers = List.of(args[++i].split(","));
            } else if ("--sf".equals(args[i]) && i + 1 < args.length) {
                sf = Double.parseDouble(args[++i]);
            } else if ("--bench".equals(args[i]) && i + 1 < args.length) {
                bench = args[++i].toLowerCase(Locale.ROOT);
            } else if ("--skip-load".equals(args[i])) {
                skipLoad = true;
            }
        }

        System.err.println("╔══════════════════════════════════════════════════════════════════╗");
        System.err.println("║   Prism Distributed Benchmark — Java Coordinator + Rust Workers  ║");
        System.err.println("╚══════════════════════════════════════════════════════════════════╝");
        System.err.println();
        System.err.printf("Workers: %s%n", workers);
        System.err.printf("Scale factor: %.3f (lineitem=%d rows, orders=%d rows)%n",
                sf, (int)(6_000_000 * sf), (int)(1_500_000 * sf));
        System.err.printf("Bench suite: %s%n", bench);
        System.err.printf("Skip data load: %s%n", skipLoad);
        System.err.println();

        try (PrismFlightExecutor executor = new PrismFlightExecutor(workers)) {
            // Step 1: Verify workers are alive
            System.err.println("━━━ Checking worker connectivity ━━━");
            for (int i = 0; i < executor.workerCount(); i++) {
                boolean alive = executor.ping(i);
                System.err.printf("  Worker %d (%s): %s%n", i, workers.get(i),
                        alive ? "OK" : "UNREACHABLE");
                if (!alive) {
                    System.err.println("ERROR: Worker unreachable. Start workers first.");
                    System.exit(1);
                }
            }
            System.err.println();

            int numWorkers = executor.workerCount();
            if (!skipLoad && shouldRun(bench, "legacy")) {
                System.err.println("━━━ Generating and distributing TPC-H data ━━━");
                long genStart = System.nanoTime();
                distributeLineitem(executor, sf, numWorkers);
                distributeOrders(executor, sf, numWorkers);
                double genMs = (System.nanoTime() - genStart) / 1_000_000.0;
                System.err.printf("  Data distribution complete in %.2fms%n%n", genMs);
            }

            if (shouldRun(bench, "legacy")) {
                runLegacyBenchmarks(executor);
            }

            if (shouldRun(bench, "distributed")) {
                runDistributedBenchmarks(executor, sf);
            }

            System.err.println();
            System.err.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.err.println("Benchmark complete.");
        }
    }

    private static boolean shouldRun(String bench, String target) {
        return "all".equals(bench) || target.equals(bench);
    }

    private static void runLegacyBenchmarks(PrismFlightExecutor executor) throws Exception {
        String[] queries = {"q1", "q3", "q6"};
        String[] queryNames = {"TPC-H Q1 (filter→agg→sort)", "TPC-H Q3 (filter→join→agg→sort)",
                "TPC-H Q6 (compound filter→global agg)"};

        System.err.println("━━━ Running legacy benchmark queries ━━━");
        for (int q = 0; q < queries.length; q++) {
            String queryId = queries[q];
            String queryName = queryNames[q];
            int numWorkers = executor.workerCount();

            System.err.printf("%n  %s%n", queryName);

            Map<String, String> tables = new HashMap<>();
            tables.put("lineitem", "data/lineitem");
            if ("q3".equals(queryId)) {
                tables.put("orders", "data/orders");
            }

            long queryStart = System.nanoTime();
            String[] resultKeys = new String[numWorkers];
            for (int w = 0; w < numWorkers; w++) {
                String resultKey = String.format("result/%s/w%d", queryId, w);
                String cmd = buildCommand(queryId, tables, resultKey);
                executor.executeQuery(w, cmd);
                resultKeys[w] = resultKey;
            }
            double execMs = (System.nanoTime() - queryStart) / 1_000_000.0;

            long fetchStart = System.nanoTime();
            long totalResultRows = 0;
            for (int w = 0; w < numWorkers; w++) {
                byte[] resultIpc = executor.fetchResults(w, resultKeys[w]);
                int rows = countRowsInIpc(resultIpc);
                totalResultRows += rows;
                System.err.printf("    Worker %d: %d result rows%n", w, rows);
            }
            double fetchMs = (System.nanoTime() - fetchStart) / 1_000_000.0;

            double totalMs = execMs + fetchMs;
            System.err.printf("    Execution: %.2fms, Fetch: %.2fms, Total: %.2fms%n",
                    execMs, fetchMs, totalMs);
            System.err.printf("    Total result rows (before merge): %d%n", totalResultRows);
        }
    }

    private static void runDistributedBenchmarks(PrismFlightExecutor executor, double sf) throws Exception {
        System.err.println();
        System.err.println("━━━ Running pushed distributed join+aggregate benchmarks ━━━");
        String runKeyPrefix = "bench/" + UUID.randomUUID();
        String lineitemKey = runKeyPrefix + "/lineitem";
        String ordersKey = runKeyPrefix + "/orders";
        distributeJoinBenchmarkData(executor, sf, lineitemKey, ordersKey);

        DistributedRevenueResult revenueResult = executeDistributedRevenueBenchmark(
                executor,
                runKeyPrefix + "/join-revenue",
                sf,
                lineitemKey,
                ordersKey);
        System.err.printf("%n  TPC-H join revenue benchmark (pushed Join + GROUP BY)%n");
        System.err.printf("    Reducer worker: %d (%s)%n",
                revenueResult.reducerIndex(), executor.workerAddress(revenueResult.reducerIndex()));
        System.err.printf("    Execution: %.2fms, Fetch: %.2fms, Total: %.2fms%n",
                revenueResult.executionMs(), revenueResult.fetchMs(), revenueResult.executionMs() + revenueResult.fetchMs());
        for (RevenueRow row : revenueResult.rows()) {
            System.err.printf(
                    Locale.ROOT,
                    "    status=%s revenue=%.3f count=%d%n",
                    row.status(),
                    row.revenue(),
                    row.count());
        }

        DistributedAvgResult avgResult = executeDistributedAvgBenchmark(
                executor,
                runKeyPrefix + "/join-avg",
                sf,
                lineitemKey,
                ordersKey);
        System.err.printf("%n  Join AVG benchmark (distributed correctness check)%n");
        System.err.printf("    Reducer worker: %d (%s)%n",
                avgResult.reducerIndex(), executor.workerAddress(avgResult.reducerIndex()));
        System.err.printf("    Execution: %.2fms, Fetch: %.2fms, Total: %.2fms%n",
                avgResult.executionMs(), avgResult.fetchMs(), avgResult.executionMs() + avgResult.fetchMs());
        for (AvgRow row : avgResult.rows()) {
            System.err.printf(Locale.ROOT, "    status=%s avg=%.3f%n", row.status(), row.avg());
        }
    }

    private static DistributedRevenueResult executeDistributedRevenueBenchmark(
            PrismFlightExecutor executor,
            String queryId,
            double sf,
            String lineitemKey,
            String ordersKey) throws Exception {
        QueryExecution execution = executeDistributedQuery(
                executor,
                queryId,
                buildDistributedRevenuePlan(),
                lineitemKey,
                ordersKey);
        long fetchStart = System.nanoTime();
        List<RevenueRow> rows = readRevenueRows(executor, execution);
        double fetchMs = (System.nanoTime() - fetchStart) / 1_000_000.0;
        validateRevenueRows(rows, expectedRevenueRows(sf, executor.workerCount()));
        return new DistributedRevenueResult(
                execution.reducerIndex(),
                rows,
                execution.executionMs(),
                fetchMs);
    }

    private static DistributedAvgResult executeDistributedAvgBenchmark(
            PrismFlightExecutor executor,
            String queryId,
            double sf,
            String lineitemKey,
            String ordersKey) throws Exception {
        QueryExecution execution = executeDistributedQuery(
                executor,
                queryId,
                buildDistributedAvgPlan(),
                lineitemKey,
                ordersKey);
        long fetchStart = System.nanoTime();
        List<AvgRow> rows = readAvgRows(executor, execution);
        double fetchMs = (System.nanoTime() - fetchStart) / 1_000_000.0;
        validateAvgRows(rows, expectedAvgRows(sf, executor.workerCount()));
        return new DistributedAvgResult(
                execution.reducerIndex(),
                rows,
                execution.executionMs(),
                fetchMs);
    }

    private static List<RevenueRow> readRevenueRows(PrismFlightExecutor executor, QueryExecution execution) throws Exception {
        List<RevenueRow> rows = new ArrayList<>();
        try (FlightStream stream = executor.fetchResultStream(execution.reducerIndex(), execution.reducerResultKey())) {
            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot();
                VarCharVector status = (VarCharVector) root.getVector(0);
                Float8Vector revenue = (Float8Vector) root.getVector(1);
                BigIntVector count = (BigIntVector) root.getVector(2);
                for (int row = 0; row < root.getRowCount(); row++) {
                    rows.add(new RevenueRow(
                            new String(status.get(row), StandardCharsets.UTF_8),
                            revenue.get(row),
                            count.get(row)));
                }
            }
        }
        rows.sort(Comparator.comparing(RevenueRow::status));
        return rows;
    }

    private static List<AvgRow> readAvgRows(PrismFlightExecutor executor, QueryExecution execution) throws Exception {
        List<AvgRow> rows = new ArrayList<>();
        try (FlightStream stream = executor.fetchResultStream(execution.reducerIndex(), execution.reducerResultKey())) {
            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot();
                VarCharVector status = (VarCharVector) root.getVector(0);
                Float8Vector avg = (Float8Vector) root.getVector(1);
                for (int row = 0; row < root.getRowCount(); row++) {
                    rows.add(new AvgRow(
                            new String(status.get(row), StandardCharsets.UTF_8),
                            avg.get(row)));
                }
            }
        }
        rows.sort(Comparator.comparing(AvgRow::status));
        return rows;
    }

    private static QueryExecution executeDistributedQuery(
            PrismFlightExecutor executor,
            String queryId,
            PrismPlanNode plan,
            String lineitemKey,
            String ordersKey) throws Exception {
        int workerCount = executor.workerCount();
        int reducerIndex = reducerIndexFor(queryId, workerCount);
        String planB64 = Base64.getEncoder().encodeToString(SubstraitSerializer.serialize(plan));
        String[] resultKeys = new String[workerCount];
        CompletableFuture<?>[] futures = new CompletableFuture<?>[workerCount];

        long queryStart = System.nanoTime();
        for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
            final int wi = workerIndex;
            resultKeys[wi] = String.format(Locale.ROOT, "result/%s/w%d", queryId, wi);
            String commandJson = buildDistributedCommand(
                    planB64,
                    resultKeys[wi],
                    lineitemKey,
                    ordersKey,
                    queryId,
                    wi,
                    reducerIndex,
                    workerCount,
                    executor.workerAddress(reducerIndex));
            futures[wi] = CompletableFuture.runAsync(() -> {
                try {
                    executor.executeQuery(wi, commandJson);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        CompletableFuture.allOf(futures).join();
        double executionMs = (System.nanoTime() - queryStart) / 1_000_000.0;
        return new QueryExecution(reducerIndex, resultKeys[reducerIndex], executionMs);
    }

    private static int reducerIndexFor(String queryId, int workerCount) {
        return Math.floorMod(queryId.hashCode(), workerCount);
    }

    private static String buildDistributedCommand(
            String planB64,
            String resultKey,
            String lineitemKey,
            String ordersKey,
            String queryId,
            int workerIndex,
            int reducerIndex,
            int workerCount,
            String reducerEndpoint) throws Exception {
        ObjectNode command = MAPPER.createObjectNode();
        command.put("substrait_plan_b64", planB64);
        command.put("result_key", resultKey);

        ObjectNode tables = command.putObject("tables");
        putTableSpec(tables, "lineitem", lineitemKey);
        putTableSpec(tables, "orders", ordersKey);

        ObjectNode dist = command.putObject("distributed_aggregate");
        dist.put("query_id", queryId);
        dist.put("role", workerIndex == reducerIndex ? "reducer" : "producer");
        dist.put("worker_index", workerIndex);
        dist.put("reducer_index", reducerIndex);
        dist.put("reducer_endpoint", reducerEndpoint);
        dist.put("expected_workers", workerCount);
        dist.put("timeout_ms", 30_000);

        return MAPPER.writeValueAsString(command);
    }

    private static PrismPlanNode buildDistributedRevenuePlan() {
        PrismPlanNode.Scan lineitem = new PrismPlanNode.Scan("lineitem", List.of(
                new PrismPlanNode.ColumnRef("l_orderkey", 0, "BIGINT"),
                new PrismPlanNode.ColumnRef("l_partkey", 1, "BIGINT"),
                new PrismPlanNode.ColumnRef("l_suppkey", 2, "BIGINT"),
                new PrismPlanNode.ColumnRef("l_linenumber", 3, "INTEGER"),
                new PrismPlanNode.ColumnRef("l_quantity", 4, "DOUBLE"),
                new PrismPlanNode.ColumnRef("l_extendedprice", 5, "DOUBLE"),
                new PrismPlanNode.ColumnRef("l_discount", 6, "DOUBLE"),
                new PrismPlanNode.ColumnRef("l_tax", 7, "DOUBLE"),
                new PrismPlanNode.ColumnRef("l_returnflag", 8, "VARCHAR"),
                new PrismPlanNode.ColumnRef("l_linestatus", 9, "VARCHAR")));
        PrismPlanNode.Scan orders = new PrismPlanNode.Scan("orders", List.of(
                new PrismPlanNode.ColumnRef("o_orderkey", 0, "BIGINT"),
                new PrismPlanNode.ColumnRef("o_custkey", 1, "BIGINT"),
                new PrismPlanNode.ColumnRef("o_orderstatus", 2, "VARCHAR"),
                new PrismPlanNode.ColumnRef("o_totalprice", 3, "DOUBLE"),
                new PrismPlanNode.ColumnRef("o_orderpriority", 4, "VARCHAR")));

        PrismPlanNode join = new PrismPlanNode.Join(lineitem, orders, "INNER", List.of(0), List.of(0));
        PrismPlanNode project = new PrismPlanNode.Project(
                join,
                List.of(12),
                List.of(new PrismPlanNode.ScalarExprNode.ArithmeticCall(
                        "MULTIPLY",
                        List.of(
                                new PrismPlanNode.ScalarExprNode.ColumnRef(5),
                                new PrismPlanNode.ScalarExprNode.ArithmeticCall(
                                        "SUBTRACT",
                                        List.of(
                                                new PrismPlanNode.ScalarExprNode.Literal(1.0, "DOUBLE"),
                                                new PrismPlanNode.ScalarExprNode.ColumnRef(6)))))));

        return new PrismPlanNode.Aggregate(
                project,
                List.of(0),
                List.of(
                        new PrismPlanNode.AggregateExpr("SUM", 1, "agg_0"),
                        new PrismPlanNode.AggregateExpr("COUNT", 1, "agg_1")));
    }

    private static PrismPlanNode buildDistributedAvgPlan() {
        PrismPlanNode.Scan lineitem = new PrismPlanNode.Scan("lineitem", List.of(
                new PrismPlanNode.ColumnRef("l_orderkey", 0, "BIGINT"),
                new PrismPlanNode.ColumnRef("l_partkey", 1, "BIGINT"),
                new PrismPlanNode.ColumnRef("l_suppkey", 2, "BIGINT"),
                new PrismPlanNode.ColumnRef("l_linenumber", 3, "INTEGER"),
                new PrismPlanNode.ColumnRef("l_quantity", 4, "DOUBLE"),
                new PrismPlanNode.ColumnRef("l_extendedprice", 5, "DOUBLE"),
                new PrismPlanNode.ColumnRef("l_discount", 6, "DOUBLE"),
                new PrismPlanNode.ColumnRef("l_tax", 7, "DOUBLE"),
                new PrismPlanNode.ColumnRef("l_returnflag", 8, "VARCHAR"),
                new PrismPlanNode.ColumnRef("l_linestatus", 9, "VARCHAR")));
        PrismPlanNode.Scan orders = new PrismPlanNode.Scan("orders", List.of(
                new PrismPlanNode.ColumnRef("o_orderkey", 0, "BIGINT"),
                new PrismPlanNode.ColumnRef("o_custkey", 1, "BIGINT"),
                new PrismPlanNode.ColumnRef("o_orderstatus", 2, "VARCHAR"),
                new PrismPlanNode.ColumnRef("o_totalprice", 3, "DOUBLE"),
                new PrismPlanNode.ColumnRef("o_orderpriority", 4, "VARCHAR")));
        PrismPlanNode join = new PrismPlanNode.Join(lineitem, orders, "INNER", List.of(0), List.of(0));
        return new PrismPlanNode.Aggregate(
                join,
                List.of(12),
                List.of(new PrismPlanNode.AggregateExpr("AVG", 5, "agg_0")));
    }

    private static void distributeJoinBenchmarkData(
            PrismFlightExecutor executor,
            double sf,
            String lineitemKey,
            String ordersKey) throws Exception {
        int numWorkers = executor.workerCount();
        int totalLineitem = (int) (6_000_000 * sf);
        int totalOrders = (int) (1_500_000 * sf);
        int lineitemPerWorker = totalLineitem / numWorkers;
        int ordersPerWorker = totalOrders / numWorkers;
        String[] flags = {"A", "N", "R"};
        String[] statuses = {"F", "O", "P"};
        String[] priorities = {"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"};

        System.err.printf(
                "  Preparing co-partitioned join benchmark data: lineitem=%d rows, orders=%d rows%n",
                totalLineitem,
                totalOrders);

        for (int workerIndex = 0; workerIndex < numWorkers; workerIndex++) {
            int lineitemStart = workerIndex * lineitemPerWorker;
            int lineitemEnd = (workerIndex == numWorkers - 1) ? totalLineitem : (workerIndex + 1) * lineitemPerWorker;
            int localLineitemCount = lineitemEnd - lineitemStart;

            int ordersStart = workerIndex * ordersPerWorker;
            int ordersEnd = (workerIndex == numWorkers - 1) ? totalOrders : (workerIndex + 1) * ordersPerWorker;
            int localOrdersCount = ordersEnd - ordersStart;

            for (int offset = 0; offset < localLineitemCount; offset += BENCH_CHUNK_ROWS) {
                int chunkSize = Math.min(BENCH_CHUNK_ROWS, localLineitemCount - offset);
                byte[] lineitemIpc = generateCoPartitionedLineitemIpc(
                        lineitemStart + offset,
                        chunkSize,
                        offset,
                        ordersStart,
                        localOrdersCount,
                        flags,
                        statuses);
                executor.sendData(workerIndex, lineitemKey, lineitemIpc);
            }
            for (int offset = 0; offset < localOrdersCount; offset += BENCH_CHUNK_ROWS) {
                int chunkSize = Math.min(BENCH_CHUNK_ROWS, localOrdersCount - offset);
                byte[] ordersIpc = generateOrdersIpc(ordersStart + offset, chunkSize, statuses, priorities);
                executor.sendData(workerIndex, ordersKey, ordersIpc);
            }
            System.err.printf(
                    "    Worker %d: lineitem=%d rows orders=%d rows%n",
                    workerIndex,
                    localLineitemCount,
                    localOrdersCount);
        }
    }

    private static List<RevenueRow> expectedRevenueRows(double sf, int workerCount) {
        Map<String, RevenueAccumulator> expected = new TreeMap<>();
        forEachJoinBenchmarkRow(sf, workerCount, (status, extendedPrice, discount) -> {
            double revenue = extendedPrice * (1.0 - discount);
            expected.computeIfAbsent(status, ignored -> new RevenueAccumulator()).add(revenue);
        });

        List<RevenueRow> rows = new ArrayList<>();
        for (Map.Entry<String, RevenueAccumulator> entry : expected.entrySet()) {
            rows.add(new RevenueRow(entry.getKey(), entry.getValue().revenue, entry.getValue().count));
        }
        return rows;
    }

    private static List<AvgRow> expectedAvgRows(double sf, int workerCount) {
        Map<String, AvgAccumulator> expected = new TreeMap<>();
        forEachJoinBenchmarkRow(sf, workerCount, (status, extendedPrice, discount) ->
                expected.computeIfAbsent(status, ignored -> new AvgAccumulator()).add(extendedPrice));

        List<AvgRow> rows = new ArrayList<>();
        for (Map.Entry<String, AvgAccumulator> entry : expected.entrySet()) {
            rows.add(new AvgRow(entry.getKey(), entry.getValue().avg()));
        }
        return rows;
    }

    private static void forEachJoinBenchmarkRow(double sf, int workerCount, JoinBenchmarkRowConsumer consumer) {
        int totalLineitem = (int) (6_000_000 * sf);
        int totalOrders = (int) (1_500_000 * sf);
        int lineitemPerWorker = totalLineitem / workerCount;
        int ordersPerWorker = totalOrders / workerCount;
        for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
            int lineitemStart = workerIndex * lineitemPerWorker;
            int lineitemEnd = (workerIndex == workerCount - 1) ? totalLineitem : (workerIndex + 1) * lineitemPerWorker;
            int ordersStart = workerIndex * ordersPerWorker;
            int ordersEnd = (workerIndex == workerCount - 1) ? totalOrders : (workerIndex + 1) * ordersPerWorker;
            int localOrdersCount = ordersEnd - ordersStart;

            if (localOrdersCount == 0) {
                continue;
            }

            for (int row = lineitemStart; row < lineitemEnd; row++) {
                int orderKey = ordersStart + ((row - lineitemStart) % localOrdersCount);
                String status = switch (orderKey % 3) {
                    case 0 -> "F";
                    case 1 -> "O";
                    default -> "P";
                };
                double extendedPrice = (row % 100_000) * 0.99 + 900.0;
                double discount = (row % 11) * 0.01;
                consumer.accept(status, extendedPrice, discount);
            }
        }
    }

    private static void validateRevenueRows(List<RevenueRow> actual, List<RevenueRow> expected) {
        final double revenueTolerance = 1e-2;
        if (actual.size() != expected.size()) {
            throw new IllegalStateException("unexpected revenue row count: expected " + expected.size() + " but found " + actual.size());
        }
        for (int i = 0; i < actual.size(); i++) {
            RevenueRow actualRow = actual.get(i);
            RevenueRow expectedRow = expected.get(i);
            if (!actualRow.status().equals(expectedRow.status())
                    || Math.abs(actualRow.revenue() - expectedRow.revenue()) > revenueTolerance
                    || actualRow.count() != expectedRow.count()) {
                throw new IllegalStateException("unexpected revenue row: expected " + expectedRow + " but found " + actualRow);
            }
        }
    }

    private static void validateAvgRows(List<AvgRow> actual, List<AvgRow> expected) {
        if (actual.size() != expected.size()) {
            throw new IllegalStateException("unexpected avg row count: expected " + expected.size() + " but found " + actual.size());
        }
        for (int i = 0; i < actual.size(); i++) {
            AvgRow actualRow = actual.get(i);
            AvgRow expectedRow = expected.get(i);
            if (!actualRow.status().equals(expectedRow.status())
                    || Math.abs(actualRow.avg() - expectedRow.avg()) > 1e-6) {
                throw new IllegalStateException("unexpected avg row: expected " + expectedRow + " but found " + actualRow);
            }
        }
    }

    private static String buildCommand(String queryId, Map<String, String> tables, String resultKey) {
        try {
            ObjectNode command = MAPPER.createObjectNode();
            command.put("query", queryId);
            command.put("result_key", resultKey);

            ObjectNode tablesNode = command.putObject("tables");
            for (var entry : tables.entrySet()) {
                putTableSpec(tablesNode, entry.getKey(), entry.getValue());
            }
            return MAPPER.writeValueAsString(command);
        }
        catch (Exception e) {
            throw new RuntimeException("failed to build benchmark command", e);
        }
    }

    private static void putTableSpec(ObjectNode tablesNode, String tableName, String storeKey) {
        ObjectNode spec = tablesNode.putObject(tableName);
        spec.putArray("uris");
        spec.put("store_key", storeKey);
    }

    /**
     * Generate lineitem data and distribute shards to workers via hash partitioning.
     */
    private static void distributeLineitem(PrismFlightExecutor executor, double sf, int numWorkers) throws Exception {
        int totalRows = (int) (6_000_000 * sf);
        int rowsPerWorker = totalRows / numWorkers;

        String[] flags = {"A", "N", "R"};
        String[] statuses = {"F", "O"};

        System.err.printf("  Generating lineitem: %d rows → %d per worker%n", totalRows, rowsPerWorker);

        for (int w = 0; w < numWorkers; w++) {
            int startRow = w * rowsPerWorker;
            int endRow = (w == numWorkers - 1) ? totalRows : (w + 1) * rowsPerWorker;
            int n = endRow - startRow;

            for (int offset = 0; offset < n; offset += BENCH_CHUNK_ROWS) {
                int chunkSize = Math.min(BENCH_CHUNK_ROWS, n - offset);
                byte[] ipcData = generateLineitemIpc(startRow + offset, chunkSize, flags, statuses);
                executor.sendData(w, "data/lineitem", ipcData);
            }
            System.err.printf("    Worker %d: sent %d rows%n", w, n);
        }
    }

    /**
     * Generate orders data and distribute shards to workers.
     */
    private static void distributeOrders(PrismFlightExecutor executor, double sf, int numWorkers) throws Exception {
        int totalRows = (int) (1_500_000 * sf);
        int rowsPerWorker = totalRows / numWorkers;

        String[] statuses = {"F", "O", "P"};
        String[] priorities = {"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"};

        System.err.printf("  Generating orders: %d rows → %d per worker%n", totalRows, rowsPerWorker);

        for (int w = 0; w < numWorkers; w++) {
            int startRow = w * rowsPerWorker;
            int endRow = (w == numWorkers - 1) ? totalRows : (w + 1) * rowsPerWorker;
            int n = endRow - startRow;

            for (int offset = 0; offset < n; offset += BENCH_CHUNK_ROWS) {
                int chunkSize = Math.min(BENCH_CHUNK_ROWS, n - offset);
                byte[] ipcData = generateOrdersIpc(startRow + offset, chunkSize, statuses, priorities);
                executor.sendData(w, "data/orders", ipcData);
            }
            System.err.printf("    Worker %d: sent %d rows%n", w, n);
        }
    }

    private static byte[] generateLineitemIpc(int startRow, int n, String[] flags, String[] statuses) throws Exception {
        Schema schema = new Schema(List.of(
                Field.nullable("l_orderkey", new ArrowType.Int(64, true)),
                Field.nullable("l_partkey", new ArrowType.Int(64, true)),
                Field.nullable("l_suppkey", new ArrowType.Int(64, true)),
                Field.nullable("l_linenumber", new ArrowType.Int(32, true)),
                Field.nullable("l_quantity", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                Field.nullable("l_extendedprice", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                Field.nullable("l_discount", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                Field.nullable("l_tax", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                Field.nullable("l_returnflag", new ArrowType.Utf8()),
                Field.nullable("l_linestatus", new ArrowType.Utf8())
        ));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, ALLOC)) {
            BigIntVector orderkey = (BigIntVector) root.getVector("l_orderkey");
            BigIntVector partkey = (BigIntVector) root.getVector("l_partkey");
            BigIntVector suppkey = (BigIntVector) root.getVector("l_suppkey");
            IntVector linenumber = (IntVector) root.getVector("l_linenumber");
            Float8Vector quantity = (Float8Vector) root.getVector("l_quantity");
            Float8Vector extprice = (Float8Vector) root.getVector("l_extendedprice");
            Float8Vector discount = (Float8Vector) root.getVector("l_discount");
            Float8Vector tax = (Float8Vector) root.getVector("l_tax");
            VarCharVector returnflag = (VarCharVector) root.getVector("l_returnflag");
            VarCharVector linestatus = (VarCharVector) root.getVector("l_linestatus");

            root.allocateNew();

            for (int i = 0; i < n; i++) {
                int row = startRow + i;
                orderkey.setSafe(i, row);
                partkey.setSafe(i, row % 200_000);
                suppkey.setSafe(i, row % 10_000);
                linenumber.setSafe(i, row % 7 + 1);
                quantity.setSafe(i, (row % 50 + 1));
                extprice.setSafe(i, (row % 100_000) * 0.99 + 900.0);
                discount.setSafe(i, (row % 11) * 0.01);
                tax.setSafe(i, (row % 9) * 0.01);
                returnflag.setSafe(i, flags[row % flags.length].getBytes());
                linestatus.setSafe(i, statuses[row % statuses.length].getBytes());
            }

            root.setRowCount(n);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return baos.toByteArray();
        }
    }

    private static byte[] generateCoPartitionedLineitemIpc(
            int startRow,
            int n,
            int shardOffset,
            int ordersStart,
            int localOrdersCount,
            String[] flags,
            String[] statuses) throws Exception {
        Schema schema = new Schema(List.of(
                Field.nullable("l_orderkey", new ArrowType.Int(64, true)),
                Field.nullable("l_partkey", new ArrowType.Int(64, true)),
                Field.nullable("l_suppkey", new ArrowType.Int(64, true)),
                Field.nullable("l_linenumber", new ArrowType.Int(32, true)),
                Field.nullable("l_quantity", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                Field.nullable("l_extendedprice", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                Field.nullable("l_discount", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                Field.nullable("l_tax", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                Field.nullable("l_returnflag", new ArrowType.Utf8()),
                Field.nullable("l_linestatus", new ArrowType.Utf8())
        ));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, ALLOC)) {
            BigIntVector orderkey = (BigIntVector) root.getVector("l_orderkey");
            BigIntVector partkey = (BigIntVector) root.getVector("l_partkey");
            BigIntVector suppkey = (BigIntVector) root.getVector("l_suppkey");
            IntVector linenumber = (IntVector) root.getVector("l_linenumber");
            Float8Vector quantity = (Float8Vector) root.getVector("l_quantity");
            Float8Vector extprice = (Float8Vector) root.getVector("l_extendedprice");
            Float8Vector discount = (Float8Vector) root.getVector("l_discount");
            Float8Vector tax = (Float8Vector) root.getVector("l_tax");
            VarCharVector returnflag = (VarCharVector) root.getVector("l_returnflag");
            VarCharVector linestatus = (VarCharVector) root.getVector("l_linestatus");

            root.allocateNew();

            for (int i = 0; i < n; i++) {
                int row = startRow + i;
                long localOrderKey = localOrdersCount == 0
                        ? row
                        : ordersStart + ((shardOffset + i) % localOrdersCount);
                orderkey.setSafe(i, localOrderKey);
                partkey.setSafe(i, row % 200_000);
                suppkey.setSafe(i, row % 10_000);
                linenumber.setSafe(i, row % 7 + 1);
                quantity.setSafe(i, (row % 50 + 1));
                extprice.setSafe(i, (row % 100_000) * 0.99 + 900.0);
                discount.setSafe(i, (row % 11) * 0.01);
                tax.setSafe(i, (row % 9) * 0.01);
                returnflag.setSafe(i, flags[row % flags.length].getBytes(StandardCharsets.UTF_8));
                linestatus.setSafe(i, statuses[(int) (localOrderKey % statuses.length)].getBytes(StandardCharsets.UTF_8));
            }

            root.setRowCount(n);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return baos.toByteArray();
        }
    }

    private static byte[] generateOrdersIpc(int startRow, int n, String[] statuses, String[] priorities) throws Exception {
        Schema schema = new Schema(List.of(
                Field.nullable("o_orderkey", new ArrowType.Int(64, true)),
                Field.nullable("o_custkey", new ArrowType.Int(64, true)),
                Field.nullable("o_orderstatus", new ArrowType.Utf8()),
                Field.nullable("o_totalprice", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                Field.nullable("o_orderpriority", new ArrowType.Utf8())
        ));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, ALLOC)) {
            BigIntVector orderkey = (BigIntVector) root.getVector("o_orderkey");
            BigIntVector custkey = (BigIntVector) root.getVector("o_custkey");
            VarCharVector orderstatus = (VarCharVector) root.getVector("o_orderstatus");
            Float8Vector totalprice = (Float8Vector) root.getVector("o_totalprice");
            VarCharVector orderpriority = (VarCharVector) root.getVector("o_orderpriority");

            root.allocateNew();

            for (int i = 0; i < n; i++) {
                int row = startRow + i;
                orderkey.setSafe(i, row);
                custkey.setSafe(i, row % 150_000);
                orderstatus.setSafe(i, statuses[row % statuses.length].getBytes());
                totalprice.setSafe(i, row * 2.5 + 100.0);
                orderpriority.setSafe(i, priorities[row % priorities.length].getBytes());
            }

            root.setRowCount(n);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return baos.toByteArray();
        }
    }

    private static int countRowsInIpc(byte[] ipcData) {
        try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(ipcData);
             org.apache.arrow.vector.ipc.ArrowStreamReader reader =
                     new org.apache.arrow.vector.ipc.ArrowStreamReader(bais, ALLOC)) {
            int total = 0;
            while (reader.loadNextBatch()) {
                total += reader.getVectorSchemaRoot().getRowCount();
            }
            return total;
        } catch (Exception e) {
            return -1;
        }
    }

    private record RevenueRow(String status, double revenue, long count) {}

    private record AvgRow(String status, double avg) {}

    private record QueryExecution(int reducerIndex, String reducerResultKey, double executionMs) {}

    private record DistributedRevenueResult(int reducerIndex, List<RevenueRow> rows, double executionMs, double fetchMs) {}

    private record DistributedAvgResult(int reducerIndex, List<AvgRow> rows, double executionMs, double fetchMs) {}

    private static final class RevenueAccumulator {
        private double revenue;
        private long count;

        private void add(double delta) {
            revenue += delta;
            count++;
        }
    }

    private static final class AvgAccumulator {
        private double sum;
        private long count;

        private void add(double value) {
            sum += value;
            count++;
        }

        private double avg() {
            return count == 0 ? 0.0 : sum / count;
        }
    }

    @FunctionalInterface
    private interface JoinBenchmarkRowConsumer {
        void accept(String status, double extendedPrice, double discount);
    }
}
