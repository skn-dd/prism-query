package io.prism.plugin.events;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-query Prism execution metrics accumulated by {@link io.prism.plugin.PrismPageSource}
 * during query execution, and consumed by {@link PrismEventListener} on query completion.
 *
 * <p>This is the in-memory payload behind the per-query entry in
 * {@link PrismQueryStatsRegistry}. Instances are keyed by the Trino query ID
 * (from {@code ConnectorSession.getQueryId()}).
 *
 * <p>All mutators are safe to call concurrently from multiple splits of the
 * same query, since a single query's page sources may run in parallel on a
 * Trino worker.
 */
public final class PrismQueryStats {

    private final String queryId;
    private volatile boolean accelerationUsed;
    private final AtomicLong bytesScanned = new AtomicLong();
    private final AtomicLong rowsProduced = new AtomicLong();
    private final AtomicLong rustRuntimeMs = new AtomicLong();
    private final AtomicLong substraitPlanBytes = new AtomicLong();
    private final Set<String> workerIds =
            Collections.synchronizedSet(new LinkedHashSet<>());
    private volatile String fallbackReason;

    public PrismQueryStats(String queryId) {
        this.queryId = queryId;
    }

    public String getQueryId() {
        return queryId;
    }

    public boolean isAccelerationUsed() {
        return accelerationUsed;
    }

    public void markAccelerationUsed() {
        this.accelerationUsed = true;
    }

    public long getBytesScanned() {
        return bytesScanned.get();
    }

    public void addBytesScanned(long delta) {
        if (delta > 0) {
            bytesScanned.addAndGet(delta);
        }
    }

    public long getRowsProduced() {
        return rowsProduced.get();
    }

    public void addRowsProduced(long delta) {
        if (delta > 0) {
            rowsProduced.addAndGet(delta);
        }
    }

    public long getRustRuntimeMs() {
        return rustRuntimeMs.get();
    }

    public void addRustRuntimeMs(long delta) {
        if (delta > 0) {
            rustRuntimeMs.addAndGet(delta);
        }
    }

    public long getSubstraitPlanBytes() {
        return substraitPlanBytes.get();
    }

    /**
     * Records the (maximum observed) shipped Substrait plan size. Multiple splits
     * for the same query ship the same plan, so taking the max is a safe reducer
     * and avoids double-counting.
     */
    public void recordSubstraitPlanBytes(long bytes) {
        if (bytes <= 0) {
            return;
        }
        long current;
        do {
            current = substraitPlanBytes.get();
            if (bytes <= current) {
                return;
            }
        } while (!substraitPlanBytes.compareAndSet(current, bytes));
    }

    public Set<String> getWorkerIds() {
        synchronized (workerIds) {
            return new LinkedHashSet<>(workerIds);
        }
    }

    public void addWorkerId(String workerId) {
        if (workerId != null && !workerId.isEmpty()) {
            workerIds.add(workerId);
        }
    }

    public String getFallbackReason() {
        return fallbackReason;
    }

    public void setFallbackReason(String fallbackReason) {
        this.fallbackReason = fallbackReason;
    }
}
