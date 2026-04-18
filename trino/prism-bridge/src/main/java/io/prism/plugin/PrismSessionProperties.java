package io.prism.plugin;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;

/**
 * Registry of Trino session properties exposed by the Prism connector.
 *
 * <p>All per-query tunables for Prism acceleration surface through Trino's
 * {@code Connector.getSessionProperties()} SPI. Cluster-wide defaults may be
 * provided via catalog properties (see
 * {@link #fromCatalogConfig(Map)}); per-session overrides flow through
 * {@code SET SESSION prism.&lt;property&gt;}.
 *
 * <p>Principle: every property registered here has a concrete consumer in the
 * current codebase (or is immediately consumed as part of the registration
 * slice). Do not register hypothetical knobs — they are a parallel universe.
 */
public final class PrismSessionProperties {

    // Property names — kept as constants so callers don't pass raw strings.
    public static final String ACCELERATION_ENABLED = "acceleration_enabled";
    public static final String MEMORY_BUDGET_GB = "memory_budget_gb";
    public static final String MAX_FILES_PER_SPLIT = "max_files_per_split";
    public static final String PREFER_SINGLE_WORKER = "prefer_single_worker";

    // Corresponding catalog-property keys. Trino namespaces catalog properties
    // with the connector name at the config-file level (the catalog file is
    // `prism.properties`), so the values inside the file are bare names —
    // except Trino also accepts `prism.<name>` for clarity. We accept both
    // `acceleration_enabled` and `prism.acceleration_enabled` as catalog keys.
    static final String CATALOG_PREFIX = "prism.";

    // Defaults — used when neither catalog config nor session override is set.
    static final boolean DEFAULT_ACCELERATION_ENABLED = true;
    static final int DEFAULT_MEMORY_BUDGET_GB = 0;          // 0 = unlimited
    static final int DEFAULT_MAX_FILES_PER_SPLIT = 64;
    static final boolean DEFAULT_PREFER_SINGLE_WORKER = false;

    private final List<PropertyMetadata<?>> sessionProperties;

    public PrismSessionProperties() {
        this(Map.of());
    }

    /**
     * @param catalogConfig the raw catalog-properties map from
     *     {@code PrismConnectorFactory.create}. Values here set the default
     *     seen by a session that doesn't {@code SET SESSION} the property.
     */
    public PrismSessionProperties(Map<String, String> catalogConfig) {
        boolean defaultAccel = boolCatalog(catalogConfig, ACCELERATION_ENABLED, DEFAULT_ACCELERATION_ENABLED);
        int defaultMemBudget = intCatalog(catalogConfig, MEMORY_BUDGET_GB, DEFAULT_MEMORY_BUDGET_GB);
        int defaultMaxFiles = intCatalog(catalogConfig, MAX_FILES_PER_SPLIT, DEFAULT_MAX_FILES_PER_SPLIT);
        boolean defaultPreferSingle = boolCatalog(catalogConfig, PREFER_SINGLE_WORKER, DEFAULT_PREFER_SINGLE_WORKER);

        this.sessionProperties = List.of(
                PropertyMetadata.booleanProperty(
                        ACCELERATION_ENABLED,
                        "Master toggle for Prism pushdown. When false, the connector still "
                                + "resolves tables but declines all pushdowns, so queries run on "
                                + "standard Trino execution. Use as a per-session kill switch.",
                        defaultAccel,
                        false),
                PropertyMetadata.integerProperty(
                        MEMORY_BUDGET_GB,
                        "Per-query memory budget in GiB enforced by Rust workers. "
                                + "0 means unlimited (worker-local default applies). "
                                + "Surfaces in the execute command JSON sent to each worker.",
                        defaultMemBudget,
                        false),
                PropertyMetadata.integerProperty(
                        MAX_FILES_PER_SPLIT,
                        "Upper bound on data files bundled into a single split. "
                                + "Tunable for Iceberg/Delta workloads once catalog delegation lands. "
                                + "Currently reserved; no split planner consumes it yet.",
                        defaultMaxFiles,
                        false),
                PropertyMetadata.booleanProperty(
                        PREFER_SINGLE_WORKER,
                        "Testing / debug toggle. When true, Prism routes the query to a single "
                                + "worker even in a multi-worker cluster, bypassing the fan-out "
                                + "merge paths in PrismPageSource. Useful for isolating per-worker "
                                + "correctness issues.",
                        defaultPreferSingle,
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties() {
        return sessionProperties;
    }

    // ===== Typed accessors =====

    public static boolean isAccelerationEnabled(ConnectorSession session) {
        return session.getProperty(ACCELERATION_ENABLED, Boolean.class);
    }

    public static int getMemoryBudgetGb(ConnectorSession session) {
        Integer v = session.getProperty(MEMORY_BUDGET_GB, Integer.class);
        return v == null ? DEFAULT_MEMORY_BUDGET_GB : v;
    }

    public static int getMaxFilesPerSplit(ConnectorSession session) {
        Integer v = session.getProperty(MAX_FILES_PER_SPLIT, Integer.class);
        return v == null ? DEFAULT_MAX_FILES_PER_SPLIT : v;
    }

    public static boolean isPreferSingleWorker(ConnectorSession session) {
        return session.getProperty(PREFER_SINGLE_WORKER, Boolean.class);
    }

    // ===== Catalog-default parsing =====
    // Catalog keys may appear either as bare names (e.g. `acceleration_enabled=false`)
    // or prefixed (`prism.acceleration_enabled=false`). Both resolve here.

    private static boolean boolCatalog(Map<String, String> cfg, String name, boolean fallback) {
        String v = lookup(cfg, name);
        if (v == null) return fallback;
        return Boolean.parseBoolean(v.trim());
    }

    private static int intCatalog(Map<String, String> cfg, String name, int fallback) {
        String v = lookup(cfg, name);
        if (v == null) return fallback;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid integer for catalog property '" + name + "': " + v, e);
        }
    }

    private static String lookup(Map<String, String> cfg, String name) {
        if (cfg == null) return null;
        String v = cfg.get(name);
        if (v != null) return v;
        return cfg.get(CATALOG_PREFIX + name);
    }

    /** Convenience factory mirroring the name used in docs. */
    public static PrismSessionProperties fromCatalogConfig(Map<String, String> catalogConfig) {
        return new PrismSessionProperties(catalogConfig);
    }
}
