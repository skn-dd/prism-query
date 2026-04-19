package io.prism.exchange;

import io.trino.spi.exchange.ExchangeSinkHandle;

import java.util.Objects;

/**
 * Identifies a single sink slot inside a Prism exchange.
 *
 * <p>Trino calls {@code Exchange.addSink(taskPartitionId)} once per upstream
 * task that will write into this exchange. The returned handle is then
 * {@link io.trino.spi.exchange.Exchange#instantiateSink instantiated} per
 * attempt, producing an {@link io.trino.spi.exchange.ExchangeSinkInstanceHandle}.</p>
 *
 * <p>The {@code taskPartitionId} here is <b>Trino's upstream task partition</b>,
 * not the sink's output partition. A single sink writes to many output partitions
 * via {@link io.trino.spi.exchange.ExchangeSink#add(int, io.airlift.slice.Slice)}.</p>
 */
public final class PrismExchangeSinkHandle implements ExchangeSinkHandle {

    private final int taskPartitionId;

    public PrismExchangeSinkHandle(int taskPartitionId) {
        this.taskPartitionId = taskPartitionId;
    }

    public int getTaskPartitionId() {
        return taskPartitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PrismExchangeSinkHandle that)) return false;
        return taskPartitionId == that.taskPartitionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskPartitionId);
    }

    @Override
    public String toString() {
        return "PrismSinkHandle[task=" + taskPartitionId + "]";
    }
}
