package io.prism.exchange;

import io.trino.spi.Plugin;
import io.trino.spi.exchange.ExchangeManagerFactory;

import java.util.List;

/**
 * Trino plugin entry point.
 *
 * <p>Installed into {@code plugin/prism-exchange/} on the Trino coordinator
 * and workers; discovered via Java SPI
 * ({@code META-INF/services/io.trino.spi.Plugin}).</p>
 *
 * <p>Activating the manager requires
 * {@code etc/exchange-manager.properties} like:</p>
 * <pre>
 * exchange-manager.name=prism-flight
 * exchange.prism.workers=prism-worker-0:50051,prism-worker-1:50051
 * exchange.prism.tls.enabled=true
 * exchange.prism.tls.server-ca-path=/etc/prism/tls/ca.pem
 * exchange.prism.tls.client-cert-path=/etc/prism/tls/client.pem
 * exchange.prism.tls.client-key-path=/etc/prism/tls/client.key
 * </pre>
 */
public final class PrismExchangePlugin implements Plugin {

    @Override
    public Iterable<ExchangeManagerFactory> getExchangeManagerFactories() {
        return List.of(new PrismExchangeManagerFactory());
    }
}
