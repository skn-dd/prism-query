package io.prism.exchange;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Parsed configuration for the Prism exchange manager.
 *
 * <p>Trino reads {@code etc/exchange-manager.properties} at the coordinator
 * and passes the key/value map to {@link PrismExchangeManagerFactory#create}.
 * This class converts that into a typed bundle.</p>
 *
 * <p>Supported keys (mirror {@code prism.*} keys in the {@code prism-bridge}
 * connector so operators configure TLS once):</p>
 * <pre>
 * exchange.prism.workers                  host:port[,host:port]*    required
 * exchange.prism.tls.enabled              true|false                default true
 * exchange.prism.tls.server-ca-path       filesystem path           required if tls.enabled
 * exchange.prism.tls.client-cert-path     filesystem path           required for mTLS
 * exchange.prism.tls.client-key-path      filesystem path           required for mTLS
 * exchange.prism.tls.server-name-override string                    optional
 * exchange.prism.partitions-per-worker    int                       default 4
 * </pre>
 *
 * <p>The {@code partitions-per-worker} multiplier is a hint for the sink→worker
 * mapping when Trino requests more output partitions than workers. See
 * {@link PrismExchangeManager} for the full partitioning strategy.</p>
 */
public final class PrismExchangeConfig {

    public static final String PROP_WORKERS = "exchange.prism.workers";
    public static final String PROP_TLS_ENABLED = "exchange.prism.tls.enabled";
    public static final String PROP_TLS_SERVER_CA = "exchange.prism.tls.server-ca-path";
    public static final String PROP_TLS_CLIENT_CERT = "exchange.prism.tls.client-cert-path";
    public static final String PROP_TLS_CLIENT_KEY = "exchange.prism.tls.client-key-path";
    public static final String PROP_TLS_SERVER_NAME = "exchange.prism.tls.server-name-override";
    public static final String PROP_PARTITIONS_PER_WORKER = "exchange.prism.partitions-per-worker";

    private final List<String> workers;
    private final PrismFlightTlsOptions tlsOptions;
    private final int partitionsPerWorker;

    private PrismExchangeConfig(List<String> workers,
                                PrismFlightTlsOptions tlsOptions,
                                int partitionsPerWorker) {
        this.workers = List.copyOf(workers);
        this.tlsOptions = tlsOptions;
        this.partitionsPerWorker = partitionsPerWorker;
    }

    public static PrismExchangeConfig fromMap(Map<String, String> config) {
        String workersStr = config.get(PROP_WORKERS);
        if (workersStr == null || workersStr.isBlank()) {
            throw new IllegalArgumentException(
                    PROP_WORKERS + " is required; expected host:port[,host:port]*");
        }
        List<String> workers = Arrays.stream(workersStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
        if (workers.isEmpty()) {
            throw new IllegalArgumentException(PROP_WORKERS + " must list at least one worker");
        }
        for (String w : workers) {
            String[] parts = w.split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException(
                        "worker endpoint is not in host:port form: '" + w + "'");
            }
            try {
                Integer.parseInt(parts[1]);
            }
            catch (NumberFormatException nfe) {
                throw new IllegalArgumentException(
                        "worker endpoint has non-numeric port: '" + w + "'");
            }
        }

        PrismFlightTlsOptions tls = readTls(config);

        int partitionsPerWorker = parseInt(config.get(PROP_PARTITIONS_PER_WORKER), 4);
        if (partitionsPerWorker < 1) {
            throw new IllegalArgumentException(
                    PROP_PARTITIONS_PER_WORKER + " must be >= 1, got " + partitionsPerWorker);
        }

        return new PrismExchangeConfig(workers, tls, partitionsPerWorker);
    }

    private static PrismFlightTlsOptions readTls(Map<String, String> config) {
        boolean enabled = parseBool(config.get(PROP_TLS_ENABLED), /*default=*/ true);
        if (!enabled) {
            return PrismFlightTlsOptions.plaintext();
        }

        String caStr = config.get(PROP_TLS_SERVER_CA);
        if (caStr == null || caStr.isBlank()) {
            throw new IllegalArgumentException(
                    PROP_TLS_SERVER_CA + " is required when " + PROP_TLS_ENABLED + "=true");
        }
        Path ca = Paths.get(caStr);

        String serverName = config.get(PROP_TLS_SERVER_NAME);
        if (serverName != null && serverName.isBlank()) {
            serverName = null;
        }

        String certStr = config.get(PROP_TLS_CLIENT_CERT);
        String keyStr = config.get(PROP_TLS_CLIENT_KEY);
        boolean hasCert = certStr != null && !certStr.isBlank();
        boolean hasKey = keyStr != null && !keyStr.isBlank();
        if (hasCert ^ hasKey) {
            throw new IllegalArgumentException(
                    "both " + PROP_TLS_CLIENT_CERT + " and " + PROP_TLS_CLIENT_KEY
                            + " must be set together for mTLS, or both omitted for server-auth-only TLS");
        }
        if (hasCert) {
            return PrismFlightTlsOptions.mtls(ca, Paths.get(certStr), Paths.get(keyStr), serverName);
        }
        return PrismFlightTlsOptions.serverAuthOnly(ca, serverName);
    }

    private static boolean parseBool(String raw, boolean def) {
        if (raw == null || raw.isBlank()) {
            return def;
        }
        switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "1": case "true": case "yes": case "on": return true;
            case "0": case "false": case "no": case "off": return false;
            default:
                throw new IllegalArgumentException("expected boolean, got: " + raw);
        }
    }

    private static int parseInt(String raw, int def) {
        if (raw == null || raw.isBlank()) {
            return def;
        }
        try {
            return Integer.parseInt(raw.trim());
        }
        catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("expected integer, got: " + raw);
        }
    }

    public List<String> workers() { return workers; }
    public PrismFlightTlsOptions tlsOptions() { return tlsOptions; }
    public int partitionsPerWorker() { return partitionsPerWorker; }
}
