package io.prism.plugin.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Prism's Trino {@link EventListener} implementation.
 *
 * <p><b>Design decision — how Prism-specific fields ride Trino's event envelope.</b>
 * The Trino 468 {@code QueryCompletedEvent} is immutable and does not expose a
 * {@code Map<String,String>} for connector-supplied attributes. We therefore
 * emit Prism-specific fields as a structured JSON payload logged at INFO on a
 * dedicated SLF4J logger {@code io.prism.events}, keyed by the Trino
 * {@code queryId}. Operators can fan this logger out to any existing sink
 * (file, Fluent Bit, Vector, Kafka, etc.) using the standard Log4j/SLF4J
 * bridging their cluster already has for Trino.
 *
 * <p>This means the listener is deliberately a <i>producer</i> of Prism events
 * into Trino's logging pipeline rather than a hard mutation of the Trino event
 * object. When/if a future Trino SPI version exposes a connector-attributes
 * map on {@code QueryCompletedEvent}, this class is the single place that
 * needs to change. The Prism-side registry + stats holder are unaffected.
 *
 * <p><b>Local-dev convenience vs. production audit.</b>  The SLF4J log line
 * here is the transport for Prism-specific fields. It is <i>not</i> the
 * production audit log — Trino's own {@code EventListener} mechanism is.
 * Cluster operators wire additional {@code EventListener} factories (Kafka,
 * OpenTelemetry, their own audit pipe) in {@code etc/event-listener.properties}
 * exactly as they already do for non-Prism queries. Prism's event listener
 * complements, and does not replace, that flow.
 */
public class PrismEventListener implements EventListener {

    /** Dedicated logger so operators can route Prism events independently. */
    private static final Logger PRISM_EVENTS_LOG = LoggerFactory.getLogger("io.prism.events");
    private static final Logger LOG = LoggerFactory.getLogger(PrismEventListener.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PrismQueryStatsRegistry registry;

    public PrismEventListener() {
        this(PrismQueryStatsRegistry.getInstance());
    }

    /** Visible for tests. */
    PrismEventListener(PrismQueryStatsRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void queryCreated(QueryCreatedEvent event) {
        // No-op for now: acceleration-decision context is captured lazily by
        // PrismPageSource when a split executes, not at plan time. If a future
        // change needs plan-time decision logging (e.g., "rejected pushdown
        // because session property X was false"), it belongs here.
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event) {
        String queryId = event.getMetadata().getQueryId();

        // Remove+emit: steady-state the registry only holds in-flight queries.
        Optional<PrismQueryStats> maybeStats = registry.remove(queryId);

        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("event", "prism.query_completed");
        payload.put("query_id", queryId);
        payload.put("query_state", event.getMetadata().getQueryState());
        event.getContext().getCatalog().ifPresent(c -> payload.put("catalog", c));
        event.getContext().getSchema().ifPresent(s -> payload.put("schema", s));
        payload.put("user", event.getContext().getUser());

        if (maybeStats.isPresent()) {
            PrismQueryStats stats = maybeStats.get();
            payload.put("prism.acceleration_used", stats.isAccelerationUsed());
            payload.put("prism.bytes_scanned", stats.getBytesScanned());
            payload.put("prism.rows_produced", stats.getRowsProduced());
            payload.put("prism.rust_runtime_ms", stats.getRustRuntimeMs());
            payload.put("prism.substrait_plan_bytes", stats.getSubstraitPlanBytes());
            payload.put("prism.worker_ids", String.join(",", stats.getWorkerIds()));
            if (stats.getFallbackReason() != null) {
                payload.put("prism.connector_fallback_reason", stats.getFallbackReason());
            }
        } else {
            // No Prism stats means Prism did not touch the query (not routed
            // through the connector, or rejected upstream). Emit a minimal
            // record so audit consumers see a deterministic per-query entry.
            payload.put("prism.acceleration_used", false);
        }

        try {
            PRISM_EVENTS_LOG.info(MAPPER.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            LOG.warn("Failed to serialize Prism event payload for query {}: {}",
                    queryId, e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        registry.clear();
    }
}
