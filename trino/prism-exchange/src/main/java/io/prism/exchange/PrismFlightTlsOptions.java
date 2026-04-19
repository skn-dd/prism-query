package io.prism.exchange;

import java.nio.file.Path;

/**
 * TLS options for Prism Flight clients used by the exchange manager.
 *
 * <p><b>Design note:</b> this class deliberately mirrors the API of
 * {@code io.prism.bridge.PrismFlightExecutor.TlsOptions} in {@code prism-bridge}.
 * Trino loads plugins with isolated classloaders, so we cannot simply import
 * the bridge's class — each plugin ships its own jar and the class identity
 * would not match at runtime. The two classes must stay in lockstep: same
 * property keys, same factory methods, same semantics. If you change one,
 * change the other.</p>
 *
 * <p>The proper fix is a shared {@code prism-flight-client} module; that is
 * out of scope for this change (see the exchange-manager wiring report).</p>
 */
public final class PrismFlightTlsOptions {

    private final boolean enabled;
    private final Path serverCa;             // nullable when !enabled
    private final Path clientCert;           // null for one-way TLS
    private final Path clientKey;            // null for one-way TLS
    private final String serverNameOverride; // null to derive from host

    private PrismFlightTlsOptions(boolean enabled,
                                  Path serverCa,
                                  Path clientCert,
                                  Path clientKey,
                                  String serverNameOverride) {
        this.enabled = enabled;
        this.serverCa = serverCa;
        this.clientCert = clientCert;
        this.clientKey = clientKey;
        this.serverNameOverride = serverNameOverride;
    }

    public static PrismFlightTlsOptions plaintext() {
        return new PrismFlightTlsOptions(false, null, null, null, null);
    }

    /** Mutual TLS: worker verifies client, client verifies worker. */
    public static PrismFlightTlsOptions mtls(Path serverCa, Path clientCert, Path clientKey,
                                             String serverNameOverride) {
        if (serverCa == null) {
            throw new IllegalArgumentException("serverCa is required for TLS");
        }
        if (clientCert == null || clientKey == null) {
            throw new IllegalArgumentException(
                    "clientCert and clientKey are both required for mTLS; "
                            + "use serverAuthOnly() for one-way TLS");
        }
        return new PrismFlightTlsOptions(true, serverCa, clientCert, clientKey, serverNameOverride);
    }

    /** One-way TLS: client verifies worker identity only. */
    public static PrismFlightTlsOptions serverAuthOnly(Path serverCa, String serverNameOverride) {
        if (serverCa == null) {
            throw new IllegalArgumentException("serverCa is required for TLS");
        }
        return new PrismFlightTlsOptions(true, serverCa, null, null, serverNameOverride);
    }

    public boolean isEnabled() { return enabled; }
    public boolean isMutual() { return enabled && clientCert != null && clientKey != null; }
    public Path serverCa() { return serverCa; }
    public Path clientCert() { return clientCert; }
    public Path clientKey() { return clientKey; }
    public String serverNameOverride() { return serverNameOverride; }
}
