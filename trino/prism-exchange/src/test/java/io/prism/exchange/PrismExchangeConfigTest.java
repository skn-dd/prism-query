package io.prism.exchange;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrismExchangeConfigTest {

    @Test
    void missingWorkersIsRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PrismExchangeConfig.fromMap(Map.of()));
        assertTrue(e.getMessage().contains("exchange.prism.workers"));
    }

    @Test
    void malformedEndpointIsRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PrismExchangeConfig.fromMap(Map.of(
                        PrismExchangeConfig.PROP_WORKERS, "not-host-port",
                        PrismExchangeConfig.PROP_TLS_ENABLED, "false")));
        assertTrue(e.getMessage().contains("host:port"));
    }

    @Test
    void nonNumericPortIsRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PrismExchangeConfig.fromMap(Map.of(
                        PrismExchangeConfig.PROP_WORKERS, "host:xyz",
                        PrismExchangeConfig.PROP_TLS_ENABLED, "false")));
        assertTrue(e.getMessage().contains("non-numeric"));
    }

    @Test
    void tlsEnabledRequiresServerCa() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PrismExchangeConfig.fromMap(Map.of(
                        PrismExchangeConfig.PROP_WORKERS, "h:1",
                        PrismExchangeConfig.PROP_TLS_ENABLED, "true")));
        assertTrue(e.getMessage().contains("server-ca-path"));
    }

    @Test
    void tlsMtlsRequiresBothCertAndKey() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PrismExchangeConfig.fromMap(Map.of(
                        PrismExchangeConfig.PROP_WORKERS, "h:1",
                        PrismExchangeConfig.PROP_TLS_ENABLED, "true",
                        PrismExchangeConfig.PROP_TLS_SERVER_CA, "/tmp/ca.pem",
                        PrismExchangeConfig.PROP_TLS_CLIENT_CERT, "/tmp/cert.pem")));
        assertTrue(e.getMessage().contains("must be set together"));
    }

    @Test
    void tlsPlaintextAllowed() {
        PrismExchangeConfig cfg = PrismExchangeConfig.fromMap(Map.of(
                PrismExchangeConfig.PROP_WORKERS, "h1:1,h2:2",
                PrismExchangeConfig.PROP_TLS_ENABLED, "false"));
        assertFalse(cfg.tlsOptions().isEnabled());
        assertEquals(2, cfg.workers().size());
        assertEquals("h1:1", cfg.workers().get(0));
    }

    @Test
    void tlsServerAuthOnlyWhenCertAbsent() {
        PrismExchangeConfig cfg = PrismExchangeConfig.fromMap(Map.of(
                PrismExchangeConfig.PROP_WORKERS, "h:1",
                PrismExchangeConfig.PROP_TLS_ENABLED, "true",
                PrismExchangeConfig.PROP_TLS_SERVER_CA, "/tmp/ca.pem"));
        assertTrue(cfg.tlsOptions().isEnabled());
        assertFalse(cfg.tlsOptions().isMutual());
    }

    @Test
    void tlsMutualWhenCertAndKeyBothPresent() {
        PrismExchangeConfig cfg = PrismExchangeConfig.fromMap(Map.of(
                PrismExchangeConfig.PROP_WORKERS, "h:1",
                PrismExchangeConfig.PROP_TLS_ENABLED, "true",
                PrismExchangeConfig.PROP_TLS_SERVER_CA, "/tmp/ca.pem",
                PrismExchangeConfig.PROP_TLS_CLIENT_CERT, "/tmp/cert.pem",
                PrismExchangeConfig.PROP_TLS_CLIENT_KEY, "/tmp/key.pem"));
        assertTrue(cfg.tlsOptions().isMutual());
    }

    @Test
    void partitionsPerWorkerDefaultsToFour() {
        PrismExchangeConfig cfg = PrismExchangeConfig.fromMap(Map.of(
                PrismExchangeConfig.PROP_WORKERS, "h:1",
                PrismExchangeConfig.PROP_TLS_ENABLED, "false"));
        assertEquals(4, cfg.partitionsPerWorker());
    }

    @Test
    void partitionsPerWorkerMustBePositive() {
        assertThrows(IllegalArgumentException.class,
                () -> PrismExchangeConfig.fromMap(Map.of(
                        PrismExchangeConfig.PROP_WORKERS, "h:1",
                        PrismExchangeConfig.PROP_TLS_ENABLED, "false",
                        PrismExchangeConfig.PROP_PARTITIONS_PER_WORKER, "0")));
    }
}
