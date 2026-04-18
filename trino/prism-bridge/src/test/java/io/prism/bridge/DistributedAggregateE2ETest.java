package io.prism.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.prism.plugin.PrismPlanNode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class DistributedAggregateE2ETest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void distributedAggregateReducerReturnsMergedJoinAggregation() throws Exception {
        runDistributedAggregateScenario(buildJoinAggregatePlan(), Map.of(
                "F", new ResultRow(expectedRevenue(0, 4), 4),
                "O", new ResultRow(expectedRevenue(4, 8), 4)));
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void distributedAggregateReducerPreservesAvgSemantics() throws Exception {
        runDistributedAggregateAvgScenario();
    }

    private static void runDistributedAggregateScenario(
            PrismPlanNode plan,
            Map<String, ResultRow> expectedRows) throws Exception {
        Path repoRoot = Paths.get("").toAbsolutePath().getParent().getParent();
        Path workerBinary = repoRoot.resolve("native/target/debug/prism-worker");
        assumeTrue(Files.isExecutable(workerBinary), "native prism-worker binary not built");

        int port0 = reservePort();
        int port1 = reservePort();
        List<Process> workers = new ArrayList<>();

        try {
            workers.add(startWorker(workerBinary, port0, "worker-" + port0 + ".log"));
            workers.add(startWorker(workerBinary, port1, "worker-" + port1 + ".log"));

            try (PrismFlightExecutor executor = new PrismFlightExecutor(List.of(
                    "127.0.0.1:" + port0,
                    "127.0.0.1:" + port1))) {
                waitForWorkers(executor);
                populateShard(executor, 0, 0, 4, 0, 1);
                populateShard(executor, 1, 4, 4, 1, 1);

                String planB64 = Base64.getEncoder().encodeToString(SubstraitSerializer.serialize(plan));
                String queryId = "join-agg-e2e";
                int workerCount = 2;
                int reducerIndex = Math.floorMod(queryId.hashCode(), workerCount);
                String[] resultKeys = {
                        "result/" + UUID.randomUUID(),
                        "result/" + UUID.randomUUID()
                };

                CompletableFuture<?>[] futures = new CompletableFuture[workerCount];
                for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
                    final int wi = workerIndex;
                    ObjectNode command = MAPPER.createObjectNode();
                    command.put("substrait_plan_b64", planB64);
                    command.put("result_key", resultKeys[wi]);

                    ObjectNode tables = command.putObject("tables");
                    putTableSpec(tables, "lineitem", "tpch/lineitem");
                    putTableSpec(tables, "orders", "tpch/orders");

                    ObjectNode dist = command.putObject("distributed_aggregate");
                    dist.put("query_id", queryId);
                    dist.put("role", wi == reducerIndex ? "reducer" : "producer");
                    dist.put("worker_index", wi);
                    dist.put("reducer_index", reducerIndex);
                    dist.put("reducer_endpoint", "127.0.0.1:" + (reducerIndex == 0 ? port0 : port1));
                    dist.put("expected_workers", workerCount);
                    dist.put("timeout_ms", 10_000);

                    String commandJson = MAPPER.writeValueAsString(command);
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

                try (FlightStream stream = executor.fetchResultStream(reducerIndex, resultKeys[reducerIndex])) {
                    Map<String, ResultRow> rows = new java.util.HashMap<>();
                    while (stream.next()) {
                        var root = stream.getRoot();
                        VarCharVector status = (VarCharVector) root.getVector(0);
                        Float8Vector revenue = (Float8Vector) root.getVector(1);
                        BigIntVector count = (BigIntVector) root.getVector(2);

                        for (int row = 0; row < root.getRowCount(); row++) {
                            rows.put(
                                new String(status.get(row)),
                                new ResultRow(revenue.get(row), count.get(row)));
                        }
                    }

                    assertEquals(expectedRows.size(), rows.size());
                    for (var entry : expectedRows.entrySet()) {
                        assertResult(rows.get(entry.getKey()), entry.getValue().revenue, entry.getValue().count);
                    }
                }
            }
        }
        finally {
            for (Process worker : workers) {
                worker.destroy();
                worker.waitFor(5, TimeUnit.SECONDS);
                if (worker.isAlive()) {
                    worker.destroyForcibly();
                }
            }
        }
    }

    private static void runDistributedAggregateAvgScenario() throws Exception {
        Path repoRoot = Paths.get("").toAbsolutePath().getParent().getParent();
        Path workerBinary = repoRoot.resolve("native/target/debug/prism-worker");
        assumeTrue(Files.isExecutable(workerBinary), "native prism-worker binary not built");

        int port0 = reservePort();
        int port1 = reservePort();
        List<Process> workers = new ArrayList<>();

        try {
            workers.add(startWorker(workerBinary, port0, "worker-avg-" + port0 + ".log"));
            workers.add(startWorker(workerBinary, port1, "worker-avg-" + port1 + ".log"));

            try (PrismFlightExecutor executor = new PrismFlightExecutor(List.of(
                    "127.0.0.1:" + port0,
                    "127.0.0.1:" + port1))) {
                waitForWorkers(executor);
                populateShard(executor, 0, 0, 4, 0, 1);
                populateShard(executor, 1, 4, 4, 1, 1);

                PrismPlanNode plan = buildJoinAvgPlan();
                String planB64 = Base64.getEncoder().encodeToString(SubstraitSerializer.serialize(plan));
                String queryId = "join-avg-e2e";
                int workerCount = 2;
                int reducerIndex = Math.floorMod(queryId.hashCode(), workerCount);
                String[] resultKeys = {
                        "result/" + UUID.randomUUID(),
                        "result/" + UUID.randomUUID()
                };

                CompletableFuture<?>[] futures = new CompletableFuture[workerCount];
                for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
                    final int wi = workerIndex;
                    ObjectNode command = MAPPER.createObjectNode();
                    command.put("substrait_plan_b64", planB64);
                    command.put("result_key", resultKeys[wi]);

                    ObjectNode tables = command.putObject("tables");
                    putTableSpec(tables, "lineitem", "tpch/lineitem");
                    putTableSpec(tables, "orders", "tpch/orders");

                    ObjectNode dist = command.putObject("distributed_aggregate");
                    dist.put("query_id", queryId);
                    dist.put("role", wi == reducerIndex ? "reducer" : "producer");
                    dist.put("worker_index", wi);
                    dist.put("reducer_index", reducerIndex);
                    dist.put("reducer_endpoint", "127.0.0.1:" + (reducerIndex == 0 ? port0 : port1));
                    dist.put("expected_workers", workerCount);
                    dist.put("timeout_ms", 10_000);

                    String commandJson = MAPPER.writeValueAsString(command);
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

                try (FlightStream stream = executor.fetchResultStream(reducerIndex, resultKeys[reducerIndex])) {
                    Map<String, Double> rows = new java.util.HashMap<>();
                    while (stream.next()) {
                        var root = stream.getRoot();
                        VarCharVector status = (VarCharVector) root.getVector(0);
                        Float8Vector avg = (Float8Vector) root.getVector(1);

                        for (int row = 0; row < root.getRowCount(); row++) {
                            rows.put(new String(status.get(row)), avg.get(row));
                        }
                    }

                    assertEquals(2, rows.size());
                    assertEquals(901.485, rows.get("F"), 1e-9);
                    assertEquals(905.445, rows.get("O"), 1e-9);
                }
            }
        }
        finally {
            for (Process worker : workers) {
                worker.destroy();
                worker.waitFor(5, TimeUnit.SECONDS);
                if (worker.isAlive()) {
                    worker.destroyForcibly();
                }
            }
        }
    }

    private static void assertResult(ResultRow actual, double expectedRevenue, long expectedCount) {
        assertTrue(actual != null, "missing reducer output row");
        assertEquals(expectedRevenue, actual.revenue, 1e-9);
        assertEquals(expectedCount, actual.count);
    }

    private static void putTableSpec(ObjectNode tablesNode, String tableName, String storeKey) {
        ObjectNode spec = tablesNode.putObject(tableName);
        spec.putArray("uris");
        spec.put("store_key", storeKey);
    }

    private static double expectedRevenue(int startInclusive, int endExclusive) {
        double total = 0.0;
        for (int i = startInclusive; i < endExclusive; i++) {
            double extendedPrice = (i % 100_000) * 0.99 + 900.0;
            double discount = (i % 11) * 0.01;
            total += extendedPrice * (1.0 - discount);
        }
        return total;
    }

    private static PrismPlanNode buildJoinAggregatePlan() {
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
                new PrismPlanNode.ColumnRef("l_linestatus", 9, "VARCHAR"),
                new PrismPlanNode.ColumnRef("l_shipdate", 10, "DATE"),
                new PrismPlanNode.ColumnRef("l_commitdate", 11, "DATE"),
                new PrismPlanNode.ColumnRef("l_receiptdate", 12, "DATE")));
        PrismPlanNode.Scan orders = new PrismPlanNode.Scan("orders", List.of(
                new PrismPlanNode.ColumnRef("o_orderkey", 0, "BIGINT"),
                new PrismPlanNode.ColumnRef("o_custkey", 1, "BIGINT"),
                new PrismPlanNode.ColumnRef("o_orderstatus", 2, "VARCHAR"),
                new PrismPlanNode.ColumnRef("o_totalprice", 3, "DOUBLE"),
                new PrismPlanNode.ColumnRef("o_orderpriority", 4, "VARCHAR"),
                new PrismPlanNode.ColumnRef("o_orderdate", 5, "DATE")));

        PrismPlanNode join = new PrismPlanNode.Join(lineitem, orders, "INNER", List.of(0), List.of(0));
        PrismPlanNode project = new PrismPlanNode.Project(
                join,
                List.of(15),
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

    private static PrismPlanNode buildJoinAvgPlan() {
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
                new PrismPlanNode.ColumnRef("l_linestatus", 9, "VARCHAR"),
                new PrismPlanNode.ColumnRef("l_shipdate", 10, "DATE"),
                new PrismPlanNode.ColumnRef("l_commitdate", 11, "DATE"),
                new PrismPlanNode.ColumnRef("l_receiptdate", 12, "DATE")));
        PrismPlanNode.Scan orders = new PrismPlanNode.Scan("orders", List.of(
                new PrismPlanNode.ColumnRef("o_orderkey", 0, "BIGINT"),
                new PrismPlanNode.ColumnRef("o_custkey", 1, "BIGINT"),
                new PrismPlanNode.ColumnRef("o_orderstatus", 2, "VARCHAR"),
                new PrismPlanNode.ColumnRef("o_totalprice", 3, "DOUBLE"),
                new PrismPlanNode.ColumnRef("o_orderpriority", 4, "VARCHAR"),
                new PrismPlanNode.ColumnRef("o_orderdate", 5, "DATE")));

        PrismPlanNode join = new PrismPlanNode.Join(lineitem, orders, "INNER", List.of(0), List.of(0));
        return new PrismPlanNode.Aggregate(
                join,
                List.of(15),
                List.of(new PrismPlanNode.AggregateExpr("AVG", 5, "agg_0")));
    }

    private static void populateShard(
            PrismFlightExecutor executor,
            int workerIndex,
            int lineitemOffset,
            int lineitemCount,
            int ordersOffset,
            int ordersCount) throws Exception {
        executeDatagen(executor, workerIndex, "lineitem", lineitemOffset, lineitemCount);
        executeDatagen(executor, workerIndex, "orders", ordersOffset, ordersCount);
    }

    private static void executeDatagen(
            PrismFlightExecutor executor,
            int workerIndex,
            String table,
            int rowOffset,
            int rowCount) throws Exception {
        ObjectNode command = MAPPER.createObjectNode();
        command.put("query", "datagen");
        command.put("table", table);
        command.put("sf", 1.0);
        command.put("store_key", "tpch/" + table);
        command.put("chunk_size", rowCount);
        command.put("row_offset", rowOffset);
        command.put("row_count", rowCount);
        command.put("result_key", "result/" + UUID.randomUUID());
        ObjectNode tables = command.putObject("tables");
        putTableSpec(tables, table, "tpch/" + table);
        executor.executeQuery(workerIndex, MAPPER.writeValueAsString(command));
    }

    private static Process startWorker(Path workerBinary, int port, String logFileName) throws IOException {
        Path logDir = Paths.get("target", "worker-logs");
        Files.createDirectories(logDir);
        Path configPath = logDir.resolve(logFileName.replace(".log", ".toml"));
        Files.writeString(configPath, "[tls]\nenabled = false\n");
        ProcessBuilder builder = new ProcessBuilder(
                workerBinary.toString(),
                "--config",
                configPath.toString(),
                "--port",
                Integer.toString(port));
        builder.redirectErrorStream(true);
        builder.redirectOutput(logDir.resolve(logFileName).toFile());
        return builder.start();
    }

    private static void waitForWorkers(PrismFlightExecutor executor) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (executor.ping(0) && executor.ping(1)) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("workers did not start in time");
    }

    private static int reservePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private record ResultRow(double revenue, long count) {}
}
