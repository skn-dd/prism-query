package io.prism.plugin.events;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Process-wide registry that maps a Trino query ID to its {@link PrismQueryStats}.
 *
 * <p>Design choice: a single {@link ConcurrentHashMap} keyed by Trino query ID,
 * rather than a {@link ThreadLocal}. Rationale:
 * <ul>
 *   <li>Trino dispatches splits across a worker pool — a single query's
 *       {@code PrismPageSource} instances can run on many threads concurrently,
 *       so a {@code ThreadLocal} would shard stats per-thread instead of
 *       per-query and require a reduce step anyway.</li>
 *   <li>The {@code EventListener} callback runs on the coordinator's event
 *       dispatch thread, which has no relation to the worker threads that
 *       executed the splits. A {@code ThreadLocal} is inaccessible to it.</li>
 *   <li>{@code QueryId} is a stable, cluster-unique key exposed directly on
 *       {@code ConnectorSession} and {@code QueryMetadata}, so correlation
 *       needs no extra propagation plumbing.</li>
 * </ul>
 *
 * <p>Entries are created lazily on first mutation by {@code PrismPageSource}
 * and removed by {@link PrismEventListener#queryCompleted} after emission, so
 * the map's steady-state size is bounded by the number of in-flight queries.
 */
public final class PrismQueryStatsRegistry {

    private static final PrismQueryStatsRegistry INSTANCE = new PrismQueryStatsRegistry();

    private final ConcurrentMap<String, PrismQueryStats> stats = new ConcurrentHashMap<>();

    private PrismQueryStatsRegistry() {}

    public static PrismQueryStatsRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Returns the existing entry for {@code queryId}, creating it if absent.
     * Safe for concurrent callers; all callers see the same instance.
     */
    public PrismQueryStats getOrCreate(String queryId) {
        return stats.computeIfAbsent(queryId, PrismQueryStats::new);
    }

    public Optional<PrismQueryStats> peek(String queryId) {
        return Optional.ofNullable(stats.get(queryId));
    }

    /**
     * Removes and returns the stats entry for {@code queryId}, or empty if none.
     * Called by the event listener once it has emitted the event so the map
     * does not accumulate entries.
     */
    public Optional<PrismQueryStats> remove(String queryId) {
        return Optional.ofNullable(stats.remove(queryId));
    }

    /** Visible for tests. */
    int size() {
        return stats.size();
    }

    /** Visible for tests. */
    void clear() {
        stats.clear();
    }
}
