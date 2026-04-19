package io.prism.exchange;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Minimal in-process test double for the Rust {@code ShuffleFlightService}.
 *
 * <p>Implements only what the exchange manager needs: storage-key-addressed
 * {@code do_put} and {@code do_get} (matching the Rust semantics exactly).
 * Any divergence from the Rust implementation's contract would cause a
 * silent mismatch in production, so keep this aligned with
 * {@code native/prism-flight/src/shuffle_writer.rs}.</p>
 */
final class InMemoryShuffleFlightProducer extends NoOpFlightProducer {

    private static final class StoredPartition {
        final Schema schema;
        final List<ArrowRecordBatch> batches = new ArrayList<>();

        StoredPartition(Schema schema) {
            this.schema = schema;
        }
    }

    private final BufferAllocator allocator;
    private final Map<String, StoredPartition> store = new ConcurrentHashMap<>();

    InMemoryShuffleFlightProducer(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    int partitionCount() {
        return store.size();
    }

    boolean containsKey(String key) {
        return store.containsKey(key);
    }

    @Override
    public Runnable acceptPut(CallContext ctx, FlightStream flightStream,
                              StreamListener<PutResult> ackStream) {
        return () -> {
            FlightDescriptor descriptor = flightStream.getDescriptor();
            String key = new String(descriptor.getCommand(), StandardCharsets.UTF_8);
            StoredPartition stored = new StoredPartition(flightStream.getSchema());
            // Consume the stream: every next() advances to a new batch in the
            // stream's root; VectorUnloader captures that batch as an
            // ArrowRecordBatch we can replay on the get side.
            try (VectorSchemaRoot root = flightStream.getRoot()) {
                while (flightStream.next()) {
                    ArrowRecordBatch captured = new VectorUnloader(root).getRecordBatch();
                    stored.batches.add(captured);
                }
            }
            store.merge(key, stored, (existing, fresh) -> {
                existing.batches.addAll(fresh.batches);
                return existing;
            });
            ackStream.onCompleted();
        };
    }

    @Override
    public void getStream(CallContext ctx, Ticket ticket, ServerStreamListener listener) {
        String key = new String(ticket.getBytes(), StandardCharsets.UTF_8);
        StoredPartition stored = store.get(key);
        if (stored == null) {
            listener.error(new RuntimeException("partition not found: " + key));
            return;
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(stored.schema, allocator)) {
            listener.start(root);
            org.apache.arrow.vector.VectorLoader loader = new org.apache.arrow.vector.VectorLoader(root);
            for (ArrowRecordBatch batch : stored.batches) {
                loader.load(batch);
                listener.putNext();
            }
            listener.completed();
        }
    }

    /** Release all captured ArrowRecordBatches. Call during test teardown. */
    void closeAll() {
        for (StoredPartition p : store.values()) {
            for (ArrowRecordBatch b : p.batches) {
                b.close();
            }
        }
        store.clear();
    }
}
