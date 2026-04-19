package io.prism.exchange;

import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeManagerContext;
import io.trino.spi.exchange.ExchangeManagerFactory;

import java.util.Map;

/**
 * SPI entry point Trino calls when loading
 * {@code etc/exchange-manager.properties}. Looked up via
 * {@link io.prism.exchange.PrismExchangePlugin#getExchangeManagerFactories()}.
 *
 * <p>The factory name ({@link #getName()}) must match the
 * {@code exchange-manager.name} property in the coordinator config.</p>
 */
public final class PrismExchangeManagerFactory implements ExchangeManagerFactory {

    public static final String NAME = "prism-flight";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public ExchangeManager create(Map<String, String> config, ExchangeManagerContext context) {
        PrismExchangeConfig parsed = PrismExchangeConfig.fromMap(config);
        return new PrismExchangeManager(parsed);
    }
}
