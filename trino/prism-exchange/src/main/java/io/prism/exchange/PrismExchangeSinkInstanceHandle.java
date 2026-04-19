package io.prism.exchange;

import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;

import java.util.Objects;

/**
 * A per-attempt instantiation of a {@link PrismExchangeSinkHandle}.
 *
 * <p>Carries everything the sink needs to route writes to the right workers:
 * the exchange id (used as a storage key prefix on the worker side), the
 * sink handle, the instance attempt id, and the exchange's total output
 * partition count.</p>
 *
 * <p><b>Cross-process safety:</b> Trino serializes this handle to JSON and
 * passes it to the executor process that actually runs the sink. All fields
 * are immutable primitives or strings; no client/allocator references leak
 * across the wire.</p>
 */
public final class PrismExchangeSinkInstanceHandle implements ExchangeSinkInstanceHandle {

    private final ExchangeId exchangeId;
    private final PrismExchangeSinkHandle sinkHandle;
    private final int attemptId;
    private final int outputPartitionCount;

    public PrismExchangeSinkInstanceHandle(ExchangeId exchangeId,
                                           PrismExchangeSinkHandle sinkHandle,
                                           int attemptId,
                                           int outputPartitionCount) {
        this.exchangeId = Objects.requireNonNull(exchangeId, "exchangeId");
        this.sinkHandle = Objects.requireNonNull(sinkHandle, "sinkHandle");
        this.attemptId = attemptId;
        this.outputPartitionCount = outputPartitionCount;
    }

    public ExchangeId getExchangeId() { return exchangeId; }
    public PrismExchangeSinkHandle getSinkHandle() { return sinkHandle; }
    public int getAttemptId() { return attemptId; }
    public int getOutputPartitionCount() { return outputPartitionCount; }

    @Override
    public String toString() {
        return "PrismSinkInstance[" + exchangeId.getId()
                + ", task=" + sinkHandle.getTaskPartitionId()
                + ", attempt=" + attemptId
                + ", outputs=" + outputPartitionCount + "]";
    }
}
