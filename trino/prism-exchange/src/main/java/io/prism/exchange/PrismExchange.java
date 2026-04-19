package io.prism.exchange;

import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Metadata and lifecycle for one logical exchange across all its sinks.
 *
 * <p>Tracks:</p>
 * <ul>
 *   <li>Which upstream sink handles have been added ({@link #addSink}) and
 *       which have finished ({@link #sinkFinished}).</li>
 *   <li>The exchange-wide output partition count.</li>
 *   <li>A boolean flag signalling {@code noMoreSinks()} was called.</li>
 * </ul>
 *
 * <p>When {@link #allRequiredSinksFinished()} fires, we generate one
 * {@link PrismExchangeSourceHandle} per output partition and hand them to
 * Trino via {@link #getSourceHandles()}. The worker that owns each
 * partition is computed statically by {@link PrismPartitionRouter}.</p>
 */
public final class PrismExchange implements Exchange {

    private static final Logger LOG = LoggerFactory.getLogger(PrismExchange.class);

    private final ExchangeContext context;
    private final int outputPartitionCount;
    private final boolean preserveOrderWithinPartition;
    private final PrismFlightClientPool clientPool;
    private final PrismPartitionRouter router;

    // Upstream sink book-keeping: task partition id → attempt counter used
    // to stamp successive sink instance handles for the same slot.
    private final ConcurrentHashMap<Integer, AtomicInteger> sinkAttempts = new ConcurrentHashMap<>();
    private final Set<Integer> finishedSinks = ConcurrentHashMap.newKeySet();

    private final AtomicBoolean noMoreSinks = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean exchangeClosedOnWorkers = new AtomicBoolean(false);
    private final AtomicBoolean exchangeDroppedOnWorkers = new AtomicBoolean(false);
    private final CompletableFuture<Void> allSinksDoneFuture = new CompletableFuture<>();

    public PrismExchange(ExchangeContext context,
                         int outputPartitionCount,
                         boolean preserveOrderWithinPartition,
                         PrismFlightClientPool clientPool,
                         PrismPartitionRouter router) {
        this.context = context;
        this.outputPartitionCount = outputPartitionCount;
        this.preserveOrderWithinPartition = preserveOrderWithinPartition;
        this.clientPool = clientPool;
        this.router = router;
    }

    @Override
    public ExchangeId getId() {
        return context.getExchangeId();
    }

    @Override
    public ExchangeSinkHandle addSink(int taskPartitionId) {
        sinkAttempts.computeIfAbsent(taskPartitionId, k -> new AtomicInteger(0));
        return new PrismExchangeSinkHandle(taskPartitionId);
    }

    @Override
    public void noMoreSinks() {
        noMoreSinks.set(true);
        maybeSignalAllSinksDone();
    }

    @Override
    public CompletableFuture<ExchangeSinkInstanceHandle> instantiateSink(
            ExchangeSinkHandle sinkHandle, int attemptId) {
        PrismExchangeSinkHandle h = expect(sinkHandle);
        // We track attempts but don't gate on them — Trino manages attempt
        // semantics itself. Each attempt writes to the same storage key
        // (exchange/{id}/{partition}); the last successful finish() wins.
        sinkAttempts.computeIfAbsent(h.getTaskPartitionId(), k -> new AtomicInteger(0))
                .accumulateAndGet(attemptId, Math::max);
        return CompletableFuture.completedFuture(new PrismExchangeSinkInstanceHandle(
                getId(), h, attemptId, outputPartitionCount));
    }

    @Override
    public CompletableFuture<ExchangeSinkInstanceHandle> updateSinkInstanceHandle(
            ExchangeSinkHandle sinkHandle, int attemptId) {
        // Same handle each time — no dynamic re-routing yet.
        return instantiateSink(sinkHandle, attemptId);
    }

    @Override
    public void sinkFinished(ExchangeSinkHandle sinkHandle, int attemptId) {
        PrismExchangeSinkHandle h = expect(sinkHandle);
        finishedSinks.add(h.getTaskPartitionId());
        maybeSignalAllSinksDone();
    }

    @Override
    public void allRequiredSinksFinished() {
        // Called by Trino when every upstream task has either finished or
        // been pruned. Treat as a terminal signal even if we hadn't seen
        // finish() for every sink.
        noMoreSinks.set(true);
        maybeSignalAllSinksDone();
    }

    private void maybeSignalAllSinksDone() {
        if (noMoreSinks.get()
                && finishedSinks.containsAll(sinkAttempts.keySet())
                && !allSinksDoneFuture.isDone()) {
            allSinksDoneFuture.complete(null);
        }
    }

    @Override
    public ExchangeSourceHandleSource getSourceHandles() {
        return new PrismSourceHandleSource();
    }

    private void closeExchangeOnWorkers() {
        if (exchangeClosedOnWorkers.compareAndSet(false, true)) {
            clientPool.runExchangeActionOnAllWorkers("close_exchange", getId().getId());
        }
    }

    private void dropExchangeOnWorkers() {
        if (exchangeDroppedOnWorkers.compareAndSet(false, true)) {
            clientPool.runExchangeActionOnAllWorkers("drop_exchange", getId().getId());
        }
    }

    private PrismExchangeSinkHandle expect(ExchangeSinkHandle handle) {
        if (handle instanceof PrismExchangeSinkHandle p) {
            return p;
        }
        throw new IllegalArgumentException(
                "Expected PrismExchangeSinkHandle, got " + handle.getClass().getName());
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            dropExchangeOnWorkers();
            // The Flight client pool is shared across exchanges and closed
            // by PrismExchangeManager.shutdown(). Nothing per-exchange to
            // release — sinks/sources manage their own streams.
            if (!allSinksDoneFuture.isDone()) {
                allSinksDoneFuture.cancel(false);
            }
            LOG.debug("PrismExchange closed: id={}", getId().getId());
        }
    }

    /**
     * Provides source handles once all sinks have finished writing.
     *
     * <p>Implemented as a single-shot batch: all {@code outputPartitionCount}
     * handles are returned in one call, and {@code lastBatch=true} so Trino
     * stops polling. This matches what {@code FileSystemExchangeManager}
     * does for small exchange counts.</p>
     */
    private final class PrismSourceHandleSource implements ExchangeSourceHandleSource {
        private final AtomicBoolean delivered = new AtomicBoolean(false);

        @Override
        public CompletableFuture<ExchangeSourceHandleBatch> getNextBatch() {
            if (delivered.getAndSet(true)) {
                return CompletableFuture.completedFuture(
                        new ExchangeSourceHandleBatch(List.of(), /*lastBatch=*/ true));
            }
            return allSinksDoneFuture.thenApply(v -> {
                closeExchangeOnWorkers();
                List<ExchangeSourceHandle> handles = new ArrayList<>(outputPartitionCount);
                for (int p = 0; p < outputPartitionCount; p++) {
                    int workerIndex = router.workerFor(p);
                    handles.add(new PrismExchangeSourceHandle(
                            getId(), p, workerIndex, /*dataSizeInBytes=*/ 0L));
                }
                // preserveOrderWithinPartition is accepted but not enforced
                // in this MVP: a single worker owns each partition and our
                // Flight server writes batches in arrival order, so in
                // practice order within a partition survives. See report.
                return new ExchangeSourceHandleBatch(handles, /*lastBatch=*/ true);
            });
        }

        @Override
        public void close() {
            // no resources held beyond the parent exchange
        }
    }

    public boolean isPreserveOrderWithinPartition() {
        return preserveOrderWithinPartition;
    }
}
