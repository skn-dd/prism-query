package io.prism.exchange;

import io.airlift.slice.Slice;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Writes exchange data for one upstream sink instance.
 *
 * <p>For each output partition Trino emits (via {@link #add(int, Slice)}), we
 * open a lazy {@code startPut} stream to the worker that owns that partition
 * (as determined by {@link PrismPartitionRouter}) and push the Slice as a
 * single-row Arrow IPC batch. Flight/gRPC provides the transport-level flow
 * control, so we don't buffer whole partitions in memory — each call streams
 * one batch into the Netty pipeline and returns.</p>
 *
 * <p><b>Not thread-safe.</b> Trino invokes the sink from a single writer thread
 * per instance.</p>
 */
public final class PrismExchangeSink implements ExchangeSink {

    private static final Logger LOG = LoggerFactory.getLogger(PrismExchangeSink.class);

    private final PrismFlightClientPool clientPool;
    private final PrismPartitionRouter router;
    private final String exchangeId;
    private final int attemptId;
    private final int outputPartitionCount;

    // One open DoPut stream per output partition, created on first write.
    // Keyed by partitionId. Keyed holder owns its own Arrow VectorSchemaRoot
    // (each gRPC stream requires its own root), so teardown is per-partition.
    private final Map<Integer, PartitionWriter> writers = new HashMap<>();

    private long bytesWritten;
    private volatile boolean closed;
    private volatile Throwable failure;

    public PrismExchangeSink(PrismFlightClientPool clientPool,
                             PrismPartitionRouter router,
                             String exchangeId,
                             int attemptId,
                             int outputPartitionCount) {
        this.clientPool = clientPool;
        this.router = router;
        this.exchangeId = exchangeId;
        this.attemptId = attemptId;
        this.outputPartitionCount = outputPartitionCount;
    }

    @Override
    public boolean isHandleUpdateRequired() {
        // Our sink handles are derived entirely from the exchange id + worker
        // layout. Neither changes during query execution, so we never need
        // Trino to refresh them.
        return false;
    }

    @Override
    public void updateHandle(ExchangeSinkInstanceHandle handle) {
        // Intentionally no-op: see isHandleUpdateRequired().
    }

    @Override
    public CompletableFuture<Void> isBlocked() {
        // Backpressure is handled at the Flight/gRPC layer (Netty's channel
        // write watermarks). We don't surface it to Trino here because the
        // Arrow Flight FlightClient.startPut API is blocking on write. If
        // this becomes a bottleneck, a later revision should switch to the
        // async Flight SQL path and wire its channel watermark events into
        // this future.
        return NOT_BLOCKED;
    }

    @Override
    public void add(int partitionId, Slice data) {
        throwIfFailed();
        if (partitionId < 0 || partitionId >= outputPartitionCount) {
            throw new IllegalArgumentException(
                    "partitionId " + partitionId + " out of range [0, "
                            + outputPartitionCount + ")");
        }
        try {
            PartitionWriter w = writers.computeIfAbsent(partitionId, this::openWriter);
            w.write(data);
            bytesWritten += data.length();
        }
        catch (Throwable t) {
            failure = t;
            throw new RuntimeException(
                    "Prism exchange sink failed writing partition " + partitionId, t);
        }
    }

    private PartitionWriter openWriter(int partitionId) {
        int workerIndex = router.workerFor(partitionId);
        FlightClient client = clientPool.client(workerIndex);
        BufferAllocator allocator = clientPool.allocator();
        String storageKey = PrismPartitionRouter.storageKey(exchangeId, partitionId);

        VectorSchemaRoot root = VectorSchemaRoot.create(
                PrismExchangePayloadSchema.schema(), allocator);
        // startPut returns a listener that drives a streaming DoPut — the
        // first message carries the FlightDescriptor with our storage key,
        // subsequent putNext() calls ship additional batches.
        FlightDescriptor descriptor = FlightDescriptor.command(
                storageKey.getBytes(StandardCharsets.UTF_8));
        FlightClient.ClientStreamListener listener =
                client.startPut(descriptor, root, new AsyncPutListener());
        return new PartitionWriter(root, listener, storageKey, workerIndex, partitionId, attemptId);
    }

    @Override
    public long getMemoryUsage() {
        // Each PartitionWriter holds one small VectorSchemaRoot (a few KB of
        // Arrow buffers). Approximation is fine — Trino uses this for sizing
        // heuristics, not accounting.
        return writers.size() * 64L * 1024L;
    }

    @Override
    public synchronized CompletableFuture<Void> finish() {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        closed = true;
        try {
            for (PartitionWriter w : writers.values()) {
                w.completed();
            }
            writers.clear();
            LOG.debug("PrismExchangeSink finished: exchange={}, attempt={}, bytes={}",
                    exchangeId, attemptId, bytesWritten);
            return CompletableFuture.completedFuture(null);
        }
        catch (Throwable t) {
            failure = t;
            return CompletableFuture.failedFuture(t);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> abort() {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        closed = true;
        for (PartitionWriter w : writers.values()) {
            w.abort();
        }
        writers.clear();
        return CompletableFuture.completedFuture(null);
    }

    private void throwIfFailed() {
        if (failure != null) {
            throw new RuntimeException("Prism exchange sink has failed", failure);
        }
        if (closed) {
            throw new IllegalStateException("Prism exchange sink is closed");
        }
    }

    /** Streaming DoPut session for one (exchange, partition) pair. */
    private static final class PartitionWriter {
        private final VectorSchemaRoot root;
        private final LargeVarBinaryVector payloadVector;
        private final FlightClient.ClientStreamListener listener;
        private final String storageKey;
        private final int workerIndex;
        private final int partitionId;
        private final int attemptId;
        private boolean schemaSent;

        PartitionWriter(VectorSchemaRoot root,
                        FlightClient.ClientStreamListener listener,
                        String storageKey,
                        int workerIndex,
                        int partitionId,
                        int attemptId) {
            this.root = root;
            this.payloadVector = (LargeVarBinaryVector) root.getVector(
                    PrismExchangePayloadSchema.PAYLOAD_FIELD);
            this.listener = listener;
            this.storageKey = storageKey;
            this.workerIndex = workerIndex;
            this.partitionId = partitionId;
            this.attemptId = attemptId;
        }

        void write(Slice data) {
            // One Slice per Arrow batch. Using a 1-row batch keeps the Slice
            // boundaries intact end-to-end and avoids concatenation/copy on
            // the read side. Flight batches are amortized via gRPC HTTP/2
            // framing, so 1 row per batch is acceptable for the MVP; a
            // future optimization is to coalesce many small Slices into one
            // batch and surface batch boundaries via Arrow offsets.
            payloadVector.allocateNew(1);
            payloadVector.setSafe(0, data.getBytes());
            payloadVector.setValueCount(1);
            root.setRowCount(1);
            listener.putNext();
            if (!schemaSent) {
                schemaSent = true;
            }
            root.clear();
        }

        void completed() {
            try {
                listener.completed();
                listener.getResult();
            }
            finally {
                root.close();
            }
        }

        void abort() {
            try {
                listener.error(new RuntimeException(
                        "aborting Prism sink for " + storageKey
                                + " worker=" + workerIndex
                                + " partition=" + partitionId
                                + " attempt=" + attemptId));
            }
            catch (RuntimeException ignored) {
                // Listener may already be closed; we only care about teardown.
            }
            finally {
                root.close();
            }
        }
    }
}
