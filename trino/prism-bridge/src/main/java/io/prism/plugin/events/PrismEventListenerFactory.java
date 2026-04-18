package io.prism.plugin.events;

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.Map;

/**
 * Trino {@link EventListenerFactory} for {@link PrismEventListener}.
 *
 * <p>Registered via {@link io.prism.plugin.PrismPlugin#getEventListenerFactories()}.
 * Activated per-cluster by placing {@code event-listener.name=prism-events} in
 * {@code etc/event-listener.properties}. See
 * {@code docker/etc/event-listener.properties.example}.
 *
 * <p>Config map is currently empty — the listener has no tunables today. When
 * Wave 2b adds e.g. a Kafka sink URL or audit-retention setting, those go here.
 */
public class PrismEventListenerFactory implements EventListenerFactory {

    public static final String NAME = "prism-events";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public EventListener create(Map<String, String> config, EventListenerContext context) {
        return new PrismEventListener();
    }
}
