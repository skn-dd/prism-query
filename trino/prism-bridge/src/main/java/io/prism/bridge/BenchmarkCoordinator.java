package io.prism.bridge;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
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

    private static final BufferAllocator ALLOC = new RootAllocator(Long.MAX_VALUE);

    public static void main(String[] args) throws Exception {
        // Parse arguments
        List<String> workers = List.of("localhost:50051", "localhost:50052");
        double sf = 0.1;

        for (int i = 0; i < args.length; i++) {
            if ("--workers".equals(args[i]) && i + 1 < args.length) {
                workers = List.of(args[++i].split(","));
            } else if ("--sf".equals(args[i]) && i + 1 < args.length) {
                sf = Double.parseDouble(args[++i]);
            }
        }

        System.err.println("╔══════════════════════════════════════════════════════════════════╗");
        System.err.println("║   Prism Distributed Benchmark — Java Coordinator + Rust Workers  ║");
        System.err.println("╚══════════════════════════════════════════════════════════════════╝");
        System.err.println();
        System.err.printf("Workers: %s%n", workers);
        System.err.printf("Scale factor: %.1f (lineitem=%d rows, orders=%d rows)%n",
                sf, (int)(6_000_000 * sf), (int)(1_500_000 * sf));
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

            // Step 2: Generate and distribute TPC-H data
            System.err.println("━━━ Generating and distributing TPC-H data ━━━");
            int numWorkers = executor.workerCount();

            long genStart = System.nanoTime();
            distributeLineitem(executor, sf, numWorkers);
            distributeOrders(executor, sf, numWorkers);
            double genMs = (System.nanoTime() - genStart) / 1_000_000.0;
            System.err.printf("  Data distribution complete in %.2fms%n%n", genMs);

            // Step 3: Run benchmark queries
            String[] queries = {"q1", "q3", "q6"};
            String[] queryNames = {"TPC-H Q1 (filter→agg→sort)", "TPC-H Q3 (filter→join→agg→sort)",
                    "TPC-H Q6 (compound filter→global agg)"};

            System.err.println("━━━ Running benchmark queries ━━━");
            for (int q = 0; q < queries.length; q++) {
                String queryId = queries[q];
                String queryName = queryNames[q];

                System.err.printf("%n  %s%n", queryName);

                // Build tables map based on query
                Map<String, String> tables = new HashMap<>();
                tables.put("lineitem", "data/lineitem");
                if ("q3".equals(queryId)) {
                    tables.put("orders", "data/orders");
                }

                // Execute on all workers in parallel
                long queryStart = System.nanoTime();

                String[] resultKeys = new String[numWorkers];
                for (int w = 0; w < numWorkers; w++) {
                    String resultKey = String.format("result/%s/w%d", queryId, w);
                    String cmd = buildCommand(queryId, tables, resultKey);
                    executor.executeQuery(w, cmd);
                    resultKeys[w] = resultKey;
                }

                double execMs = (System.nanoTime() - queryStart) / 1_000_000.0;

                // Fetch results from all workers
                long fetchStart = System.nanoTime();
                long totalResultRows = 0;
                for (int w = 0; w < numWorkers; w++) {
                    byte[] resultIpc = executor.fetchResults(w, resultKeys[w]);
                    // Count rows in the result
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

            System.err.println();
            System.err.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.err.println("Benchmark complete.");
        }
    }

    private static String buildCommand(String queryId, Map<String, String> tables, String resultKey) {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"query\":\"").append(queryId).append("\",");
        sb.append("\"tables\":{");
        boolean first = true;
        for (var entry : tables.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(entry.getKey()).append("\":\"").append(entry.getValue()).append("\"");
            first = false;
        }
        sb.append("},");
        sb.append("\"result_key\":\"").append(resultKey).append("\"");
        sb.append("}");
        return sb.toString();
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

            byte[] ipcData = generateLineitemIpc(startRow, n, flags, statuses);
            executor.sendData(w, "data/lineitem", ipcData);
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

            byte[] ipcData = generateOrdersIpc(startRow, n, statuses, priorities);
            executor.sendData(w, "data/orders", ipcData);
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
}
