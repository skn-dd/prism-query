package io.prism.plugin;

import io.prism.bridge.PrismFlightExecutor;
import io.prism.bridge.PrismFlightExecutor.TlsOptions;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PrismConnectorFactory implements ConnectorFactory {

    // --- Connector property keys ---------------------------------------
    // Keep these grouped and documented so the set of supported keys is
    // obvious from the factory alone.
    //
    // prism.workers                   host:port[,host:port]*    (required)
    // prism.tls.enabled               true|false                (default true)
    // prism.tls.client-cert-path      filesystem path           (required if enabled=true; mTLS)
    // prism.tls.client-key-path       filesystem path           (required if enabled=true; mTLS)
    // prism.tls.server-ca-path        filesystem path           (required if enabled=true)
    // prism.tls.server-name-override  string                    (optional; use when host != cert CN)
    static final String PROP_WORKERS = "prism.workers";
    static final String PROP_TLS_ENABLED = "prism.tls.enabled";
    static final String PROP_TLS_CLIENT_CERT = "prism.tls.client-cert-path";
    static final String PROP_TLS_CLIENT_KEY = "prism.tls.client-key-path";
    static final String PROP_TLS_SERVER_CA = "prism.tls.server-ca-path";
    static final String PROP_TLS_SERVER_NAME = "prism.tls.server-name-override";

    @Override
    public String getName() {
        return "prism";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        String workersStr = config.getOrDefault(PROP_WORKERS, "localhost:50051");
        List<String> workers = Arrays.stream(workersStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        TlsOptions tls = readTlsOptions(config);
        PrismFlightExecutor executor = new PrismFlightExecutor(workers, tls);
        return new PrismConnector(executor);
    }

    /**
     * Map connector properties to {@link TlsOptions}. Fails fast with a
     * clear error if {@code prism.tls.enabled=true} (the default) but
     * required paths are missing — we prefer a noisy startup failure
     * over a silent plaintext fallback.
     */
    static TlsOptions readTlsOptions(Map<String, String> config) {
        boolean enabled = parseBool(config.get(PROP_TLS_ENABLED), /*default=*/ true);
        if (!enabled) {
            return TlsOptions.plaintext();
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
            return TlsOptions.tls(ca, Paths.get(certStr), Paths.get(keyStr), serverName);
        }
        return TlsOptions.serverAuthOnly(ca, serverName);
    }

    private static boolean parseBool(String raw, boolean def) {
        if (raw == null || raw.isBlank()) {
            return def;
        }
        switch (raw.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "1":
            case "true":
            case "yes":
            case "on":
                return true;
            case "0":
            case "false":
            case "no":
            case "off":
                return false;
            default:
                throw new IllegalArgumentException(
                        "expected boolean for " + PROP_TLS_ENABLED + ", got: " + raw);
        }
    }
}
