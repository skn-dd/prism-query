package io.prism.plugin;

import io.prism.bridge.PrismFlightExecutor.TlsOptions;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link PrismConnectorFactory#readTlsOptions(Map)}. These
 * cover the property-to-TlsOptions mapping that the factory applies at
 * catalog-creation time. End-to-end TLS wire tests live on the Rust
 * side; here we just pin the config surface.
 */
class PrismConnectorFactoryTlsTest {

    @Test
    void disabledExplicitlyYieldsPlaintext() {
        Map<String, String> p = new HashMap<>();
        p.put(PrismConnectorFactory.PROP_TLS_ENABLED, "false");
        TlsOptions opts = PrismConnectorFactory.readTlsOptions(p);
        assertFalse(opts.isEnabled());
        assertFalse(opts.isMutual());
    }

    @Test
    void defaultIsTlsEnabledRequiresServerCa() {
        Map<String, String> p = new HashMap<>();
        // No tls.enabled set → default true → server-ca required.
        TrinoException ex = assertThrows(
                TrinoException.class,
                () -> PrismConnectorFactory.readTlsOptions(p));
        assertEquals(StandardErrorCode.CONFIGURATION_INVALID.toErrorCode(), ex.getErrorCode());
        assertTrue(ex.getMessage().contains(PrismConnectorFactory.PROP_TLS_SERVER_CA),
                "error should mention missing server-ca-path: " + ex.getMessage());
    }

    @Test
    void serverAuthOnlyWhenCaSetButNoClientCert() {
        Map<String, String> p = new HashMap<>();
        p.put(PrismConnectorFactory.PROP_TLS_SERVER_CA, "/etc/prism/tls/ca.crt");
        TlsOptions opts = PrismConnectorFactory.readTlsOptions(p);
        assertTrue(opts.isEnabled());
        assertFalse(opts.isMutual(), "no client cert → one-way TLS");
    }

    @Test
    void fullMtlsWhenAllThreePathsProvided() {
        Map<String, String> p = new HashMap<>();
        p.put(PrismConnectorFactory.PROP_TLS_SERVER_CA, "/etc/prism/tls/ca.crt");
        p.put(PrismConnectorFactory.PROP_TLS_CLIENT_CERT, "/etc/prism/tls/client.crt");
        p.put(PrismConnectorFactory.PROP_TLS_CLIENT_KEY, "/etc/prism/tls/client.key");
        p.put(PrismConnectorFactory.PROP_TLS_SERVER_NAME, "prism-worker");
        TlsOptions opts = PrismConnectorFactory.readTlsOptions(p);
        assertTrue(opts.isEnabled());
        assertTrue(opts.isMutual());
    }

    @Test
    void mismatchedClientCertAndKeyIsRejected() {
        Map<String, String> p = new HashMap<>();
        p.put(PrismConnectorFactory.PROP_TLS_SERVER_CA, "/etc/prism/tls/ca.crt");
        p.put(PrismConnectorFactory.PROP_TLS_CLIENT_CERT, "/etc/prism/tls/client.crt");
        // key omitted
        TrinoException ex = assertThrows(
                TrinoException.class,
                () -> PrismConnectorFactory.readTlsOptions(p));
        assertEquals(StandardErrorCode.CONFIGURATION_INVALID.toErrorCode(), ex.getErrorCode());
        assertTrue(ex.getMessage().contains("must be set together"),
                "error should explain cert/key symmetry requirement: " + ex.getMessage());
    }

    @Test
    void unknownBooleanLiteralIsRejected() {
        Map<String, String> p = new HashMap<>();
        p.put(PrismConnectorFactory.PROP_TLS_ENABLED, "maybe");
        TrinoException ex = assertThrows(
                TrinoException.class,
                () -> PrismConnectorFactory.readTlsOptions(p));
        assertEquals(StandardErrorCode.CONFIGURATION_INVALID.toErrorCode(), ex.getErrorCode());
    }

    @Test
    void blankServerNameOverrideTreatedAsUnset() {
        Map<String, String> p = new HashMap<>();
        p.put(PrismConnectorFactory.PROP_TLS_SERVER_CA, "/etc/prism/tls/ca.crt");
        p.put(PrismConnectorFactory.PROP_TLS_SERVER_NAME, "   ");
        // Should not throw — blank override is just ignored.
        TlsOptions opts = PrismConnectorFactory.readTlsOptions(p);
        assertTrue(opts.isEnabled());
    }
}
