package io.prism.exchange;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrismPartitionRouterTest {

    @Test
    void modulusMapsPartitionsToWorkers() {
        PrismPartitionRouter r = new PrismPartitionRouter(4);
        assertEquals(0, r.workerFor(0));
        assertEquals(1, r.workerFor(1));
        assertEquals(2, r.workerFor(2));
        assertEquals(3, r.workerFor(3));
        assertEquals(0, r.workerFor(4));
        assertEquals(3, r.workerFor(1_000_003));
    }

    @Test
    void negativePartitionIdIsHandled() {
        PrismPartitionRouter r = new PrismPartitionRouter(4);
        // floorMod semantics — always in [0, workerCount)
        assertEquals(3, r.workerFor(-1));
        assertEquals(0, r.workerFor(-4));
    }

    @Test
    void zeroWorkersIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new PrismPartitionRouter(0));
    }

    @Test
    void storageKeyIsPartitionedByExchange() {
        assertEquals("exchange/ex-1/7", PrismPartitionRouter.storageKey("ex-1", 7));
        // Different exchange id → different key prefix → no collision
        // between concurrent queries on the worker's in-memory store.
        assertEquals("exchange/ex-2/7", PrismPartitionRouter.storageKey("ex-2", 7));
    }

    @Test
    void partitionsDistributeEvenlyAcrossWorkersForBalancedInput() {
        int workers = 5;
        int partitions = 5_000;
        PrismPartitionRouter r = new PrismPartitionRouter(workers);
        Map<Integer, Integer> counts = new HashMap<>();
        for (int p = 0; p < partitions; p++) {
            counts.merge(r.workerFor(p), 1, Integer::sum);
        }
        assertEquals(workers, counts.size());
        for (int w = 0; w < workers; w++) {
            // exactly 1000 each by modulus
            assertEquals(partitions / workers, counts.get(w));
        }
    }

    @Test
    void storageKeyIsStableForSameInputs() {
        // Cross-process agreement is the whole point — two sinks computing
        // the same key must produce byte-identical strings.
        String a = PrismPartitionRouter.storageKey("abc", 42);
        String b = PrismPartitionRouter.storageKey("abc", 42);
        assertTrue(a.equals(b));
    }
}
