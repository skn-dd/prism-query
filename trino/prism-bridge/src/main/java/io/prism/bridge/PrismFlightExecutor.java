package io.prism.bridge;

import io.trino.spi.StandardErrorCode;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Arrow Flight-based executor — connects to Rust workers over gRPC.
 *
 * <p>Replaces PrismNativeExecutor (JNI) with a network-based path:
 * Java coordinator → Arrow Flight gRPC → Rust worker process.</p>
 *
 * <p>Protocol:</p>
 * <ul>
 *   <li>DoPut: Send Arrow RecordBatches to worker (table data)</li>
 *   <li>DoAction("execute"): Send query plan for execution</li>
 *   <li>DoGet: Retrieve results from worker</li>
 * </ul>
 */
public class PrismFlightExecutor implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PrismFlightExecutor.class);

    private final BufferAllocator allocator;
    private final List<FlightClient> clients;
    private final List<String> workerAddresses;

    /**
     * Create executor connected to the specified Rust workers.
     *
     * @param workerEndpoints list of "host:port" strings
     */
    public PrismFlightExecutor(List<String> workerEndpoints) {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.clients = new ArrayList<>();
        this.workerAddresses = new ArrayList<>(workerEndpoints);

        for (String endpoint : workerEndpoints) {
            String[] parts = endpoint.split(":");
            if (parts.length != 2) {
                throw PrismErrorMapper.forCode(
                        StandardErrorCode.CONFIGURATION_INVALID,
                        "Prism worker endpoint is not in host:port form: '" + endpoint + "'");
            }
            String host = parts[0];
            int port;
            try {
                port = Integer.parseInt(parts[1]);
            } catch (NumberFormatException nfe) {
                throw PrismErrorMapper.forCode(
                        StandardErrorCode.CONFIGURATION_INVALID,
                        "Prism worker endpoint has non-numeric port: '" + endpoint + "'");
            }

            Location location = Location.forGrpcInsecure(host, port);
            FlightClient client = FlightClient.builder(allocator, location)
                    .maxInboundMessageSize(256 * 1024 * 1024)
                    .build();
            clients.add(client);
            LOG.info("Connected to Rust worker at {}", endpoint);
        }
    }

    /**
     * Return the host:port address for the given worker index,
     * or {@code null} if the index is out of range.
     */
    public String workerAddress(int workerIndex) {
        if (workerIndex < 0 || workerIndex >= workerAddresses.size()) {
            return null;
        }
        return workerAddresses.get(workerIndex);
    }

    /**
     * Send Arrow IPC data to a specific worker, stored under the given key.
     */
    public void sendData(int workerIndex, String storageKey, byte[] arrowIpcData) throws Exception {
        FlightClient client = clients.get(workerIndex);

        // Read the IPC data into VectorSchemaRoot
        try (ByteArrayInputStream bais = new ByteArrayInputStream(arrowIpcData);
             ArrowStreamReader reader = new ArrowStreamReader(bais, allocator)) {

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            reader.loadNextBatch();

            // Create a FlightDescriptor with the storage key
            FlightDescriptor descriptor = FlightDescriptor.command(storageKey.getBytes(StandardCharsets.UTF_8));

            // Use DoPut to send to the worker
            FlightClient.ClientStreamListener listener = client.startPut(descriptor, root, new AsyncPutListener());

            listener.putNext();
            while (reader.loadNextBatch()) {
                listener.putNext();
            }
            listener.completed();
            listener.getResult();

            LOG.info("Sent data to worker {} under key '{}'", workerIndex, storageKey);
        }
    }

    /**
     * Execute a query on a specific worker.
     *
     * @param workerIndex index of the target worker
     * @param commandJson JSON command string
     * @return the result key where output is stored
     */
    public String executeQuery(int workerIndex, String commandJson) throws Exception {
        FlightClient client = clients.get(workerIndex);

        Action action = new Action("execute", commandJson.getBytes(StandardCharsets.UTF_8));
        FlightStream resultStream = null;

        // DoAction returns a stream of Result messages
        var results = client.doAction(action);
        if (results.hasNext()) {
            Result result = results.next();
            String body = new String(result.getBody(), StandardCharsets.UTF_8);
            LOG.info("Worker {} execute result: {}", workerIndex, body);
            return body;
        }

        throw PrismErrorMapper.forCode(
                StandardErrorCode.GENERIC_INTERNAL_ERROR,
                "Prism executeQuery returned no result [worker=" + workerAddress(workerIndex) + "]");
    }

    /**
     * Fetch results as a FlightStream — zero-copy, no intermediate byte[] serialization.
     */
    public FlightStream fetchResultStream(int workerIndex, String resultKey) throws Exception {
        FlightClient client = clients.get(workerIndex);
        Ticket ticket = new Ticket(resultKey.getBytes(StandardCharsets.UTF_8));
        return client.getStream(ticket);
    }

    /**
     * Fetch results from a worker by key.
     *
     * @return Arrow IPC bytes of the result
     */
    public byte[] fetchResults(int workerIndex, String resultKey) throws Exception {
        FlightClient client = clients.get(workerIndex);

        Ticket ticket = new Ticket(resultKey.getBytes(StandardCharsets.UTF_8));
        FlightStream stream = client.getStream(ticket);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (VectorSchemaRoot root = stream.getRoot();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
            writer.start();
            while (stream.next()) {
                writer.writeBatch();
            }
            writer.end();
        }

        return baos.toByteArray();
    }

    /**
     * Ping a worker to check connectivity.
     */
    public boolean ping(int workerIndex) {
        try {
            FlightClient client = clients.get(workerIndex);
            Action action = new Action("ping", new byte[0]);
            var results = client.doAction(action);
            if (results.hasNext()) {
                String body = new String(results.next().getBody(), StandardCharsets.UTF_8);
                return "pong".equals(body);
            }
        } catch (Exception e) {
            LOG.warn("Ping failed for worker {}: {}", workerIndex, e.getMessage());
        }
        return false;
    }

    public int workerCount() {
        return clients.size();
    }

    @Override
    public void close() {
        for (FlightClient client : clients) {
            try {
                client.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        allocator.close();
        LOG.info("PrismFlightExecutor closed");
    }
}
