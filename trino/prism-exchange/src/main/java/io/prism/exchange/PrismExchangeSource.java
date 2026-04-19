package io.prism.exchange;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Reads exchange data for a single downstream task.
 *
 * <p>Trino hands us a list of {@link PrismExchangeSourceHandle}s via
 * {@link #addSourceHandles}. Each handle names one (exchange, partition,
 * worker) triple. We iterate them in order, issuing a {@code DoGet} against
 * the owning worker, and surface each Arrow batch's single {@code payload}
 * row as a {@link Slice} to Trino.</p>
 *
 * <p><b>Not thread-safe.</b> Trino calls {@link #read()} from a single
 * reader thread.</p>
 */
public final class PrismExchangeSource implements ExchangeSource {

    private static final Logger LOG = LoggerFactory.getLogger(PrismExchangeSource.class);

    private final PrismFlightClientPool clientPool;
    private final Deque<PrismExchangeSourceHandle> pendingHandles = new ArrayDeque<>();

    private boolean noMoreHandles;
    private boolean closed;

    private FlightStream currentStream;
    private LargeVarBinaryVector currentPayload;
    private int currentRow;
    private int currentRowCount;

    private long bytesRead;

    public PrismExchangeSource(PrismFlightClientPool clientPool) {
        this.clientPool = clientPool;
    }

    @Override
    public void addSourceHandles(List<ExchangeSourceHandle> handles) {
        for (ExchangeSourceHandle h : handles) {
            if (!(h instanceof PrismExchangeSourceHandle ph)) {
                throw new IllegalArgumentException(
                        "Expected PrismExchangeSourceHandle, got " + h.getClass().getName());
            }
            pendingHandles.add(ph);
        }
    }

    @Override
    public void noMoreSourceHandles() {
        noMoreHandles = true;
    }

    @Override
    public void setOutputSelector(ExchangeSourceOutputSelector selector) {
        // Output selectors are used by Trino FTE (fault-tolerant execution)
        // to tell the source which attempts of which tasks actually
        // committed. Prism's MVP exchange does not support speculative
        // execution — each sink writes to a unique storage key and finish()
        // seals it — so we can safely ignore the selector for the first
        // implementation. Before enabling FTE over Prism, this method must
        // start filtering source handles by the committed-attempt set.
    }

    @Override
    public CompletableFuture<Void> isBlocked() {
        // We read synchronously from the current FlightStream; flow control
        // at the Netty/gRPC layer already bounds in-flight bytes. If no
        // stream is open, we're not blocked on I/O either — we'll open the
        // next one on demand in read().
        return NOT_BLOCKED;
    }

    @Override
    public boolean isFinished() {
        return closed
                || (noMoreHandles
                        && pendingHandles.isEmpty()
                        && currentStream == null);
    }

    @Override
    public Slice read() {
        if (closed) {
            return null;
        }
        while (true) {
            if (currentStream != null && currentRow < currentRowCount) {
                byte[] payload = currentPayload.get(currentRow);
                currentRow++;
                bytesRead += payload.length;
                return Slices.wrappedBuffer(payload);
            }
            if (currentStream != null) {
                // Current batch exhausted — try to pull the next one.
                if (currentStream.next()) {
                    rebindCurrentBatch();
                    continue;
                }
                closeCurrentStream();
            }
            if (pendingHandles.isEmpty()) {
                return null; // Source is drained; isFinished() will return true.
            }
            openNextStream();
        }
    }

    private void openNextStream() {
        PrismExchangeSourceHandle h = pendingHandles.pollFirst();
        FlightClient client = clientPool.client(h.getWorkerIndex());
        String storageKey = PrismPartitionRouter.storageKey(
                h.getExchangeId().getId(), h.getPartitionId());
        Ticket ticket = new Ticket(storageKey.getBytes(StandardCharsets.UTF_8));
        FlightStream stream = client.getStream(ticket);
        if (!stream.next()) {
            // Empty partition (sink wrote zero rows). Close and move on.
            try {
                stream.close();
            }
            catch (Exception e) {
                LOG.debug("Error closing empty stream for {}: {}", storageKey, e.getMessage());
            }
            return;
        }
        currentStream = stream;
        rebindCurrentBatch();
    }

    private void rebindCurrentBatch() {
        VectorSchemaRoot root = currentStream.getRoot();
        currentPayload = (LargeVarBinaryVector) root.getVector(
                PrismExchangePayloadSchema.PAYLOAD_FIELD);
        currentRowCount = root.getRowCount();
        currentRow = 0;
    }

    private void closeCurrentStream() {
        if (currentStream != null) {
            try {
                currentStream.close();
            }
            catch (Exception e) {
                LOG.debug("Error closing Flight stream: {}", e.getMessage());
            }
            currentStream = null;
            currentPayload = null;
            currentRow = 0;
            currentRowCount = 0;
        }
    }

    @Override
    public long getMemoryUsage() {
        // The active FlightStream keeps one Arrow batch resident. This is a
        // rough upper bound; Trino uses it for back-pressure heuristics only.
        return currentRowCount == 0 ? 0L : 64L * 1024L;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        closeCurrentStream();
        pendingHandles.clear();
        LOG.debug("PrismExchangeSource closed: bytes={}", bytesRead);
    }
}
