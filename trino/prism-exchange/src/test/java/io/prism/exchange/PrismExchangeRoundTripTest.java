package io.prism.exchange;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;
import io.trino.spi.QueryId;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test: sink writes Slices over Flight into a pair of
 * in-process "worker" Flight servers, source reads them back and the
 * resulting Slice set matches what was written.
 *
 * <p>Stands in for the Rust worker so we can exercise the real
 * {@code FlightClient} code path without crossing language boundaries.</p>
 */
class PrismExchangeRoundTripTest {

    private BufferAllocator serverAllocator;
    private final List<FlightServer> servers = new ArrayList<>();
    private final List<InMemoryShuffleFlightProducer> producers = new ArrayList<>();
    private final List<String> endpoints = new ArrayList<>();
    private PrismExchangeManager manager;

    @BeforeEach
    void setUp() throws Exception {
        serverAllocator = new RootAllocator(Long.MAX_VALUE);
        // Two workers so partition→worker mapping (mod 2) is nontrivial.
        for (int i = 0; i < 2; i++) {
            InMemoryShuffleFlightProducer producer = new InMemoryShuffleFlightProducer(serverAllocator);
            FlightServer server = FlightServer.builder(
                            serverAllocator,
                            Location.forGrpcInsecure("127.0.0.1", 0),
                            producer)
                    .build()
                    .start();
            servers.add(server);
            producers.add(producer);
            endpoints.add("127.0.0.1:" + server.getPort());
        }

        PrismExchangeConfig cfg = PrismExchangeConfig.fromMap(Map.of(
                PrismExchangeConfig.PROP_WORKERS, String.join(",", endpoints),
                PrismExchangeConfig.PROP_TLS_ENABLED, "false"));
        manager = new PrismExchangeManager(cfg);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (manager != null) {
            manager.close();
        }
        for (FlightServer s : servers) {
            s.close();
        }
        for (InMemoryShuffleFlightProducer p : producers) {
            p.closeAll();
        }
        servers.clear();
        producers.clear();
        endpoints.clear();
        if (serverAllocator != null) {
            serverAllocator.close();
        }
    }

    private static ExchangeContext ctx(String exchangeId) {
        return new ExchangeContext() {
            @Override public QueryId getQueryId() { return new QueryId("query_1"); }
            @Override public ExchangeId getExchangeId() { return new ExchangeId(exchangeId); }
            @Override public io.opentelemetry.api.trace.Span getParentSpan() {
                return io.opentelemetry.api.trace.Span.getInvalid();
            }
        };
    }

    @Test
    void sinkAndSourceRoundTripPreservesPayloads() throws Exception {
        int outputPartitions = 6; // partitions > workerCount (=2), exercises mod
        Exchange exchange = manager.createExchange(ctx("ex_round_trip"), outputPartitions, false);

        // One upstream sink task
        ExchangeSinkHandle sinkHandle = exchange.addSink(0);
        exchange.noMoreSinks();
        ExchangeSinkInstanceHandle instanceHandle =
                exchange.instantiateSink(sinkHandle, 0).get();

        ExchangeSink sink = manager.createSink(instanceHandle);
        // Write one small Slice per output partition; easy to reconstruct.
        Set<String> expectedContents = new HashSet<>();
        for (int p = 0; p < outputPartitions; p++) {
            String payload = "p=" + p + ";value=" + (p * 100);
            expectedContents.add(payload);
            sink.add(p, Slices.utf8Slice(payload));
        }
        sink.finish().get();
        exchange.sinkFinished(sinkHandle, 0);
        exchange.allRequiredSinksFinished();

        // Now read everything back via the source
        ExchangeSourceHandleSource handleSource = exchange.getSourceHandles();
        ExchangeSourceHandleSource.ExchangeSourceHandleBatch batch = handleSource.getNextBatch().get();
        assertTrue(batch.lastBatch(), "source handles should fit in a single batch");
        assertEquals(outputPartitions, batch.handles().size());

        ExchangeSource source = manager.createSource();
        source.addSourceHandles(batch.handles());
        source.noMoreSourceHandles();

        Set<String> seen = new HashSet<>();
        while (!source.isFinished()) {
            Slice read = source.read();
            if (read == null) {
                continue;
            }
            seen.add(read.toStringUtf8());
        }
        source.close();
        exchange.close();

        assertEquals(expectedContents, seen, "every written payload should come back exactly once");
    }

    @Test
    void partitionsLandOnExpectedWorkers() throws Exception {
        int outputPartitions = 4; // p=0,2 → worker 0 ; p=1,3 → worker 1
        Exchange exchange = manager.createExchange(ctx("ex_routing"), outputPartitions, false);
        ExchangeSinkHandle sinkHandle = exchange.addSink(0);
        exchange.noMoreSinks();
        ExchangeSinkInstanceHandle instanceHandle =
                exchange.instantiateSink(sinkHandle, 0).get();
        ExchangeSink sink = manager.createSink(instanceHandle);
        for (int p = 0; p < outputPartitions; p++) {
            sink.add(p, Slices.utf8Slice("data-for-p" + p));
        }
        sink.finish().get();
        exchange.sinkFinished(sinkHandle, 0);
        exchange.allRequiredSinksFinished();

        // Worker 0 should hold partitions 0 and 2; worker 1 should hold 1 and 3.
        assertTrue(producers.get(0).containsKey("exchange/ex_routing/0"));
        assertTrue(producers.get(0).containsKey("exchange/ex_routing/2"));
        assertTrue(producers.get(1).containsKey("exchange/ex_routing/1"));
        assertTrue(producers.get(1).containsKey("exchange/ex_routing/3"));
        assertEquals(2, producers.get(0).partitionCount());
        assertEquals(2, producers.get(1).partitionCount());

        exchange.close();
    }

    @Test
    void multipleSlicesPerPartitionAreAllReturned() throws Exception {
        int outputPartitions = 2;
        Exchange exchange = manager.createExchange(ctx("ex_multi"), outputPartitions, false);
        ExchangeSinkHandle sinkHandle = exchange.addSink(0);
        exchange.noMoreSinks();
        ExchangeSinkInstanceHandle instanceHandle =
                exchange.instantiateSink(sinkHandle, 0).get();

        ExchangeSink sink = manager.createSink(instanceHandle);
        Set<String> expected = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String v = "p0-msg-" + i;
            expected.add(v);
            sink.add(0, Slices.utf8Slice(v));
        }
        for (int i = 0; i < 10; i++) {
            String v = "p1-msg-" + i;
            expected.add(v);
            sink.add(1, Slices.utf8Slice(v));
        }
        sink.finish().get();
        exchange.sinkFinished(sinkHandle, 0);
        exchange.allRequiredSinksFinished();

        ExchangeSourceHandleSource.ExchangeSourceHandleBatch batch =
                exchange.getSourceHandles().getNextBatch().get();
        ExchangeSource source = manager.createSource();
        source.addSourceHandles(batch.handles());
        source.noMoreSourceHandles();

        Set<String> seen = new HashSet<>();
        while (!source.isFinished()) {
            Slice s = source.read();
            if (s != null) {
                seen.add(s.toStringUtf8());
            }
        }
        source.close();
        exchange.close();

        assertEquals(expected, seen);
    }

    @Test
    void sourceHandlesCarryExpectedMetadata() throws Exception {
        int outputPartitions = 3;
        Exchange exchange = manager.createExchange(ctx("ex_handles"), outputPartitions, false);
        ExchangeSinkHandle sinkHandle = exchange.addSink(0);
        exchange.noMoreSinks();
        exchange.instantiateSink(sinkHandle, 0).get();
        exchange.sinkFinished(sinkHandle, 0);
        exchange.allRequiredSinksFinished();

        List<ExchangeSourceHandle> handles = exchange.getSourceHandles().getNextBatch().get().handles();
        assertEquals(outputPartitions, handles.size());
        for (ExchangeSourceHandle h : handles) {
            assertTrue(h instanceof PrismExchangeSourceHandle);
            PrismExchangeSourceHandle p = (PrismExchangeSourceHandle) h;
            assertNotNull(p.getExchangeId());
            assertEquals("ex_handles", p.getExchangeId().getId());
            assertTrue(p.getWorkerIndex() >= 0 && p.getWorkerIndex() < 2);
            // Partition p→worker index agrees with the router
            assertEquals(p.getPartitionId() % 2, p.getWorkerIndex());
        }

        exchange.close();
    }
}
