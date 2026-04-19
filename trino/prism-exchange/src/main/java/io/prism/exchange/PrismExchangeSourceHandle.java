package io.prism.exchange;

import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSourceHandle;

import java.util.Objects;

/**
 * Describes a single output partition that a downstream task should read.
 *
 * <p>Encodes the three things the source needs to issue {@code DoGet}:</p>
 * <ol>
 *   <li>{@link #getExchangeId()} — the storage-key prefix on the worker
 *       (see {@link PrismPartitionRouter#storageKey}).</li>
 *   <li>{@link #getPartitionId()} — which partition to fetch.</li>
 *   <li>{@link #getWorkerIndex()} — which worker holds it, resolved at sink time
 *       by {@link PrismPartitionRouter}. The source uses this to pick the
 *       correct {@link org.apache.arrow.flight.FlightClient} from its pool.</li>
 * </ol>
 *
 * <p>The worker index is stable because {@code workerFor(p) = p mod workerCount},
 * and both sink and source agree on {@code workerCount} by reading the same
 * {@code exchange.prism.workers} list from the coordinator config. If workers
 * are added/removed mid-query the exchange is invalid — same constraint as
 * Trino's native hash partitioning.</p>
 */
public final class PrismExchangeSourceHandle implements ExchangeSourceHandle {

    private final ExchangeId exchangeId;
    private final int partitionId;
    private final int workerIndex;
    private final long dataSizeInBytes;

    public PrismExchangeSourceHandle(ExchangeId exchangeId,
                                     int partitionId,
                                     int workerIndex,
                                     long dataSizeInBytes) {
        this.exchangeId = Objects.requireNonNull(exchangeId, "exchangeId");
        this.partitionId = partitionId;
        this.workerIndex = workerIndex;
        this.dataSizeInBytes = dataSizeInBytes;
    }

    public ExchangeId getExchangeId() { return exchangeId; }

    @Override
    public int getPartitionId() { return partitionId; }

    public int getWorkerIndex() { return workerIndex; }

    @Override
    public long getDataSizeInBytes() { return dataSizeInBytes; }

    @Override
    public long getRetainedSizeInBytes() {
        // No Arrow buffers are pinned by this handle — it's just metadata.
        return 0;
    }

    @Override
    public String toString() {
        return "PrismSourceHandle[" + exchangeId.getId()
                + ", partition=" + partitionId
                + ", worker=" + workerIndex
                + ", bytes=" + dataSizeInBytes + "]";
    }
}
