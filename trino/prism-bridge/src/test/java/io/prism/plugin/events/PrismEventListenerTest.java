package io.prism.plugin.events;

import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PrismEventListener}.
 *
 * <p>These verify the listener's emission shape against a populated /
 * unpopulated {@link PrismQueryStatsRegistry}. They attach a Logback {@code ListAppender}
 * to the {@code io.prism.events} logger so the test can assert on the exact
 * JSON line produced, without needing a full Trino runtime.
 */
class PrismEventListenerTest {

    private PrismQueryStatsRegistry registry;
    private PrismEventListener listener;
    private ListAppender appender;

    @BeforeEach
    void setUp() {
        registry = newIsolatedRegistry();
        listener = new PrismEventListener(registry);
        appender = ListAppender.attach("io.prism.events");
    }

    @Test
    void emitsAllPrismFieldsWhenStatsPresent() {
        String queryId = "query-abc";
        PrismQueryStats stats = registry.getOrCreate(queryId);
        stats.markAccelerationUsed();
        stats.addBytesScanned(4096);
        stats.addRowsProduced(10);
        stats.addRustRuntimeMs(123);
        stats.recordSubstraitPlanBytes(2048);
        stats.addWorkerId("worker-0");
        stats.addWorkerId("worker-1");

        listener.queryCompleted(buildEvent(queryId, "FINISHED", "alice"));

        String line = appender.onlyMessage();
        assertTrue(line.contains("\"query_id\":\"query-abc\""), line);
        assertTrue(line.contains("\"prism.acceleration_used\":true"), line);
        assertTrue(line.contains("\"prism.bytes_scanned\":4096"), line);
        assertTrue(line.contains("\"prism.rows_produced\":10"), line);
        assertTrue(line.contains("\"prism.rust_runtime_ms\":123"), line);
        assertTrue(line.contains("\"prism.substrait_plan_bytes\":2048"), line);
        assertTrue(line.contains("\"prism.worker_ids\":\"worker-0,worker-1\""), line);
        assertTrue(line.contains("\"user\":\"alice\""), line);

        // Listener must consume-and-clear so the registry doesn't grow unboundedly.
        assertEquals(0, registry.size());
    }

    @Test
    void emitsMinimalRecordWhenNoStats() {
        listener.queryCompleted(buildEvent("query-none", "FINISHED", "bob"));

        String line = appender.onlyMessage();
        assertTrue(line.contains("\"query_id\":\"query-none\""), line);
        assertTrue(line.contains("\"prism.acceleration_used\":false"), line);
        // None of the other prism.* fields should appear.
        assertFalse(line.contains("prism.bytes_scanned"), line);
        assertFalse(line.contains("prism.rows_produced"), line);
        assertFalse(line.contains("prism.rust_runtime_ms"), line);
        assertFalse(line.contains("prism.substrait_plan_bytes"), line);
        assertFalse(line.contains("prism.worker_ids"), line);
    }

    @Test
    void emitsFallbackReasonWhenSet() {
        String queryId = "query-fallback";
        PrismQueryStats stats = registry.getOrCreate(queryId);
        stats.setFallbackReason("unsupported_operator");
        // acceleration_used stays false: we attempted but fell back

        listener.queryCompleted(buildEvent(queryId, "FINISHED", "carol"));

        String line = appender.onlyMessage();
        assertTrue(line.contains("\"prism.acceleration_used\":false"), line);
        assertTrue(line.contains("\"prism.connector_fallback_reason\":\"unsupported_operator\""),
                line);
    }

    // --- helpers ------------------------------------------------------------

    /** Trino event IDs are enough for the listener under test. */
    private static QueryCompletedEvent buildEvent(String queryId, String state, String user) {
        QueryMetadata meta = mock(QueryMetadata.class);
        when(meta.getQueryId()).thenReturn(queryId);
        when(meta.getQueryState()).thenReturn(state);

        QueryContext ctx = mock(QueryContext.class);
        when(ctx.getUser()).thenReturn(user);
        when(ctx.getCatalog()).thenReturn(Optional.of("prism"));
        when(ctx.getSchema()).thenReturn(Optional.of("default"));

        QueryCompletedEvent event = mock(QueryCompletedEvent.class);
        when(event.getMetadata()).thenReturn(meta);
        when(event.getContext()).thenReturn(ctx);
        return event;
    }

    /** Creates a fresh registry via reflection-free trick: use the singleton but clear. */
    private static PrismQueryStatsRegistry newIsolatedRegistry() {
        PrismQueryStatsRegistry r = PrismQueryStatsRegistry.getInstance();
        r.clear();
        return r;
    }

    /** Minimal Logback appender that captures log messages for a named logger. */
    static final class ListAppender
            extends ch.qos.logback.core.AppenderBase<ch.qos.logback.classic.spi.ILoggingEvent> {
        private final List<String> messages = new CopyOnWriteArrayList<>();

        static ListAppender attach(String loggerName) {
            Logger slf4j = LoggerFactory.getLogger(loggerName);
            ch.qos.logback.classic.Logger lb = (ch.qos.logback.classic.Logger) slf4j;
            lb.setLevel(ch.qos.logback.classic.Level.INFO);
            ListAppender a = new ListAppender();
            a.setContext(lb.getLoggerContext());
            a.start();
            // Detach previous appenders of this type (fresh between tests).
            lb.iteratorForAppenders().forEachRemaining(existing -> {
                if (existing instanceof ListAppender) {
                    lb.detachAppender(existing);
                }
            });
            lb.addAppender(a);
            return a;
        }

        @Override
        protected void append(ch.qos.logback.classic.spi.ILoggingEvent event) {
            messages.add(event.getFormattedMessage());
        }

        String onlyMessage() {
            assertEquals(1, messages.size(),
                    "Expected exactly one log message, got: " + messages);
            return messages.get(0);
        }
    }
}
