package io.prism.exchange;

import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * Top-level exchange manager for Prism.
 *
 * <p>Owns:</p>
 * <ul>
 *   <li>The {@link PrismFlightClientPool} (one Flight client per worker).</li>
 *   <li>The {@link PrismPartitionRouter} mapping partitionId → worker.</li>
 * </ul>
 *
 * <p>Every {@link PrismExchange} created here shares those resources. The
 * pool is closed exactly once when Trino invokes {@link #shutdown()}.</p>
 *
 * <h2>Partitioning</h2>
 *
 * <p>When Trino calls {@link #createExchange(ExchangeContext, int, boolean)}
 * with {@code outputPartitionCount=N}, we create an exchange whose
 * partition → worker map is simply {@code p mod workerCount}. We do
 * <b>not</b> enforce an upper bound on {@code N}; Trino may pick a number
 * much larger than the worker count (to match its task concurrency) and
 * that's fine — partitions are multiplexed onto workers by the modulus.
 * Each unique partition still gets its own storage key on the owning worker
 * ({@code exchange/{id}/{p}}), so Trino's partition identity is preserved.</p>
 *
 * <h2>Concurrent read/write</h2>
 *
 * <p>We return {@code true} from {@link #supportsConcurrentReadAndWrite}.
 * Prism still publishes source handles only after all sinks finish, but the
 * worker-side Flight service now has explicit exchange lifecycle actions:
 * {@code close_exchange} marks the exchange complete before readers start,
 * and {@code drop_exchange} releases worker state when the exchange is torn
 * down.</p>
 */
public final class PrismExchangeManager implements ExchangeManager, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PrismExchangeManager.class);

    private final PrismFlightClientPool clientPool;
    private final PrismPartitionRouter router;

    public PrismExchangeManager(PrismExchangeConfig config) {
        this.clientPool = new PrismFlightClientPool(config.workers(), config.tlsOptions());
        this.router = new PrismPartitionRouter(clientPool.workerCount());
        LOG.info("PrismExchangeManager initialized: workers={}, tls={}",
                clientPool.workerCount(),
                config.tlsOptions().isEnabled() ? "on" : "off");
    }

    @Override
    public Exchange createExchange(ExchangeContext context,
                                   int outputPartitionCount,
                                   boolean preserveOrderWithinPartition) {
        LOG.debug("createExchange: id={}, outputs={}, preserveOrder={}",
                context.getExchangeId().getId(),
                outputPartitionCount,
                preserveOrderWithinPartition);
        return new PrismExchange(
                context, outputPartitionCount, preserveOrderWithinPartition,
                clientPool, router);
    }

    @Override
    public ExchangeSink createSink(ExchangeSinkInstanceHandle handle) {
        if (!(handle instanceof PrismExchangeSinkInstanceHandle h)) {
            throw new IllegalArgumentException(
                    "Expected PrismExchangeSinkInstanceHandle, got " + handle.getClass().getName());
        }
        return new PrismExchangeSink(
                clientPool,
                router,
                h.getExchangeId().getId(),
                h.getAttemptId(),
                h.getOutputPartitionCount());
    }

    @Override
    public ExchangeSource createSource() {
        return new PrismExchangeSource(clientPool);
    }

    @Override
    public boolean supportsConcurrentReadAndWrite() {
        return true;
    }

    @Override
    public void shutdown() {
        close();
    }

    @Override
    public void close() {
        clientPool.close();
        LOG.info("PrismExchangeManager shut down");
    }

    // Visible for tests
    PrismPartitionRouter router() {
        return router;
    }

    PrismFlightClientPool clientPool() {
        return clientPool;
    }
}
