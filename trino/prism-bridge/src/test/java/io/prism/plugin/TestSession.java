package io.prism.plugin;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.TimeZoneKey;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Minimal ConnectorSession test double. Carries just the typed-property map used
 * by PrismSessionProperties accessors; unimplemented parts of the SPI return
 * sensible stand-ins.
 */
final class TestSession implements ConnectorSession {
    private final Map<String, Object> properties;

    private TestSession(Map<String, Object> properties) {
        this.properties = properties;
    }

    /** Build a session seeded from the defaults declared by PrismSessionProperties. */
    static TestSession withDefaults() {
        PrismSessionProperties props = new PrismSessionProperties();
        Map<String, Object> m = new HashMap<>();
        for (PropertyMetadata<?> p : props.getSessionProperties()) {
            m.put(p.getName(), p.getDefaultValue());
        }
        return new TestSession(m);
    }

    /** Build a session seeded from an arbitrary session-property registry. */
    static TestSession from(List<PropertyMetadata<?>> registry) {
        Map<String, Object> m = new HashMap<>();
        for (PropertyMetadata<?> p : registry) {
            m.put(p.getName(), p.getDefaultValue());
        }
        return new TestSession(m);
    }

    TestSession with(String name, Object value) {
        Map<String, Object> m = new HashMap<>(properties);
        m.put(name, value);
        return new TestSession(m);
    }

    @Override
    public String getQueryId() { return "test-query"; }

    @Override
    public Optional<String> getSource() { return Optional.empty(); }

    @Override
    public ConnectorIdentity getIdentity() {
        return ConnectorIdentity.ofUser("test");
    }

    @Override
    public TimeZoneKey getTimeZoneKey() { return TimeZoneKey.UTC_KEY; }

    @Override
    public Locale getLocale() { return Locale.ENGLISH; }

    @Override
    public Optional<String> getTraceToken() { return Optional.empty(); }

    @Override
    public Instant getStart() { return Instant.EPOCH; }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProperty(String name, Class<T> type) {
        Object v = properties.get(name);
        if (v == null) return null;
        return (T) v;
    }
}
