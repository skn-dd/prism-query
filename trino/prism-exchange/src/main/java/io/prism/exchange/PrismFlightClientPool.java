package io.prism.exchange;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Action;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Holds one {@link FlightClient} per worker endpoint plus the shared
 * {@link BufferAllocator}.
 *
 * <p>Intentionally mirrors the client-building logic in
 * {@code io.prism.bridge.PrismFlightExecutor} so exchange traffic and
 * query-plan traffic use the same TLS stack (Netty/OpenSSL via
 * {@code FlightClient.Builder}).</p>
 *
 * <p>This pool is created once per {@link PrismExchangeManager} instance
 * (typically one per Trino server) and shared across all sinks and sources
 * served by that manager. Arrow Flight clients are thread-safe — concurrent
 * {@code startPut} / {@code getStream} calls on the same client are allowed
 * because each produces an independent gRPC stream.</p>
 */
public final class PrismFlightClientPool implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PrismFlightClientPool.class);

    private final BufferAllocator allocator;
    private final List<String> endpoints;
    private final List<FlightClient> clients;

    public PrismFlightClientPool(List<String> workerEndpoints, PrismFlightTlsOptions tls) {
        Objects.requireNonNull(workerEndpoints, "workerEndpoints");
        Objects.requireNonNull(tls, "tls");
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.endpoints = List.copyOf(workerEndpoints);
        this.clients = new ArrayList<>(endpoints.size());
        for (String endpoint : endpoints) {
            clients.add(buildClient(endpoint, tls));
        }
    }

    private FlightClient buildClient(String endpoint, PrismFlightTlsOptions tls) {
        String[] parts = endpoint.split(":");
        // PrismExchangeConfig validates endpoint format, so parts[1] is a valid int here.
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        Location location = tls.isEnabled()
                ? Location.forGrpcTls(host, port)
                : Location.forGrpcInsecure(host, port);

        FlightClient.Builder builder = FlightClient.builder(allocator, location)
                .maxInboundMessageSize(256 * 1024 * 1024);

        if (!tls.isEnabled()) {
            return builder.build();
        }

        try (InputStream caStream = Files.newInputStream(tls.serverCa())) {
            builder = builder.useTls().trustedCertificates(caStream);
            if (tls.serverNameOverride() != null && !tls.serverNameOverride().isEmpty()) {
                builder = builder.overrideHostname(tls.serverNameOverride());
            }
            if (tls.isMutual()) {
                try (InputStream certStream = Files.newInputStream(tls.clientCert());
                     InputStream keyStream = Files.newInputStream(tls.clientKey())) {
                    builder = builder.clientCertificate(certStream, keyStream);
                    return builder.build();
                }
            }
            return builder.build();
        }
        catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to load TLS material for Prism worker " + endpoint, e);
        }
    }

    /** Worker count — also used as the modulus of the partition→worker map. */
    public int workerCount() {
        return clients.size();
    }

    /** Endpoint string for the given worker, e.g. {@code prism-worker-0:50051}. */
    public String endpoint(int workerIndex) {
        return endpoints.get(workerIndex);
    }

    public FlightClient client(int workerIndex) {
        return clients.get(workerIndex);
    }

    public BufferAllocator allocator() {
        return allocator;
    }

    public void runExchangeAction(int workerIndex, String actionType, String exchangeId) {
        Objects.requireNonNull(actionType, "actionType");
        Objects.requireNonNull(exchangeId, "exchangeId");

        FlightClient client = client(workerIndex);
        Action action = new Action(actionType, exchangeId.getBytes(StandardCharsets.UTF_8));
        try {
            var results = client.doAction(action);
            while (results.hasNext()) {
                results.next();
            }
        }
        catch (Exception e) {
            throw new IllegalStateException(
                    "Flight action " + actionType + " failed for exchange " + exchangeId
                            + " on worker " + endpoint(workerIndex),
                    e);
        }
    }

    public void runExchangeActionOnAllWorkers(String actionType, String exchangeId) {
        for (int workerIndex = 0; workerIndex < workerCount(); workerIndex++) {
            runExchangeAction(workerIndex, actionType, exchangeId);
        }
    }

    @Override
    public void close() {
        for (FlightClient c : clients) {
            try {
                c.close();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Exception e) {
                LOG.warn("Error closing Flight client: {}", e.getMessage());
            }
        }
        try {
            allocator.close();
        }
        catch (Exception e) {
            LOG.warn("Error closing Arrow allocator: {}", e.getMessage());
        }
    }
}
