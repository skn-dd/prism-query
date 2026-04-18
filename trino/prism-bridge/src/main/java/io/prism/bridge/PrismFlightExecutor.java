package io.prism.bridge;

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
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
 *
 * <h2>Transport security</h2>
 *
 * <p>When constructed with a {@link TlsOptions} that has {@code enabled=true},
 * this client uses TLS (or mTLS, when client cert/key are supplied) to reach
 * the workers. The options also carry a CA bundle and an optional server-name
 * override for cases where the worker cert's CN/SAN differs from the
 * configured host (e.g. connecting to {@code 127.0.0.1} but the cert's SAN is
 * {@code prism-worker}).</p>
 *
 * <p>Arrow Flight's {@link FlightClient.Builder} delegates to gRPC/Netty and
 * uses Netty's native TLS stack (OpenSSL/JDK provider). We do not plug in a
 * custom {@code SslContext} — the builder's
 * {@code useTls() / trustedCertificates() / clientCertificate()} surface is
 * sufficient for cert-manager-mounted material, and staying on it keeps us
 * out of the Netty shading fray.</p>
 */
public class PrismFlightExecutor implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PrismFlightExecutor.class);

    /**
     * Transport-level TLS options for {@link PrismFlightExecutor}. Immutable.
     *
     * <p>Factory methods:</p>
     * <ul>
     *   <li>{@link #plaintext()} — dev-only opt-out. No TLS on the wire.</li>
     *   <li>{@link #tls(Path, Path, Path, String)} — full mTLS.</li>
     *   <li>{@link #serverAuthOnly(Path, String)} — one-way TLS.</li>
     * </ul>
     */
    public static final class TlsOptions {
        private final boolean enabled;
        private final Path serverCa;       // may be null → system trust store
        private final Path clientCert;     // null for one-way TLS
        private final Path clientKey;      // null for one-way TLS
        private final String serverNameOverride; // null = derive from host

        private TlsOptions(boolean enabled, Path serverCa, Path clientCert, Path clientKey,
                           String serverNameOverride) {
            this.enabled = enabled;
            this.serverCa = serverCa;
            this.clientCert = clientCert;
            this.clientKey = clientKey;
            this.serverNameOverride = serverNameOverride;
        }

        public static TlsOptions plaintext() {
            return new TlsOptions(false, null, null, null, null);
        }

        /** Mutual TLS: server must verify, client presents its cert. */
        public static TlsOptions tls(Path serverCa, Path clientCert, Path clientKey,
                                     String serverNameOverride) {
            if (serverCa == null) {
                throw new IllegalArgumentException("serverCa is required for TLS");
            }
            if (clientCert == null || clientKey == null) {
                throw new IllegalArgumentException(
                        "clientCert and clientKey are both required for mTLS; " +
                        "use serverAuthOnly() for one-way TLS");
            }
            return new TlsOptions(true, serverCa, clientCert, clientKey, serverNameOverride);
        }

        /** One-way TLS: client verifies server identity only. */
        public static TlsOptions serverAuthOnly(Path serverCa, String serverNameOverride) {
            if (serverCa == null) {
                throw new IllegalArgumentException("serverCa is required for TLS");
            }
            return new TlsOptions(true, serverCa, null, null, serverNameOverride);
        }

        public boolean isEnabled() { return enabled; }
        public boolean isMutual() { return enabled && clientCert != null && clientKey != null; }
    }

    private final BufferAllocator allocator;
    private final List<FlightClient> clients;
    private final List<String> workerAddresses;

    /**
     * Plaintext-only constructor — kept for backward compatibility with
     * existing call sites (benchmarks, legacy integration tests). New code
     * should use {@link #PrismFlightExecutor(List, TlsOptions)}.
     */
    public PrismFlightExecutor(List<String> workerEndpoints) {
        this(workerEndpoints, TlsOptions.plaintext());
    }

    /**
     * Create executor connected to the specified Rust workers.
     *
     * @param workerEndpoints list of "host:port" strings
     * @param tls transport security settings (see {@link TlsOptions})
     */
    public PrismFlightExecutor(List<String> workerEndpoints, TlsOptions tls) {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.clients = new ArrayList<>();
        this.workerAddresses = new ArrayList<>(workerEndpoints);

        for (String endpoint : workerEndpoints) {
            String[] parts = endpoint.split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException(
                        "worker endpoint must be host:port, got: " + endpoint);
            }
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            FlightClient client = buildClient(host, port, tls);
            clients.add(client);
            LOG.info("Connected to Rust worker at {} (tls={}, mtls={})",
                    endpoint, tls.isEnabled(), tls.isMutual());
        }
    }

    private FlightClient buildClient(String host, int port, TlsOptions tls) {
        Location location = tls.isEnabled()
                ? Location.forGrpcTls(host, port)
                : Location.forGrpcInsecure(host, port);

        FlightClient.Builder builder = FlightClient.builder(allocator, location)
                .maxInboundMessageSize(256 * 1024 * 1024);

        if (!tls.isEnabled()) {
            return builder.build();
        }

        // Open the CA + client material as fresh InputStreams for the
        // builder to consume. The builder reads them eagerly inside
        // build(), so we can close them immediately afterwards.
        try (InputStream caStream = Files.newInputStream(tls.serverCa)) {
            builder = builder.useTls().trustedCertificates(caStream);
            if (tls.serverNameOverride != null && !tls.serverNameOverride.isEmpty()) {
                builder = builder.overrideHostname(tls.serverNameOverride);
            }
            if (tls.isMutual()) {
                try (InputStream certStream = Files.newInputStream(tls.clientCert);
                     InputStream keyStream = Files.newInputStream(tls.clientKey)) {
                    builder = builder.clientCertificate(certStream, keyStream);
                    return builder.build();
                }
            }
            return builder.build();
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to load TLS material while constructing FlightClient for "
                            + host + ":" + port + ": " + e.getMessage(), e);
        }
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

        throw new PrismExecutionException("Worker returned no result for execute action");
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
