package io.prism.plugin;

import io.prism.plugin.events.PrismEventListenerFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.List;

public class PrismPlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return List.of(new PrismConnectorFactory());
    }

    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories() {
        return List.of(new PrismEventListenerFactory());
    }
}
