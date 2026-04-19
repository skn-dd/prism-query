package io.prism.exchange;

/**
 * Maps a Trino output partition id to a Prism worker index.
 *
 * <p>Trino may request many more output partitions than there are workers
 * (partition counts up to the query engine's {@code task.concurrency}).
 * We bucket them onto workers with a deterministic {@code partitionId mod
 * workerCount}. That way:</p>
 * <ul>
 *   <li>Sinks on every coordinator worker agree on where partition {@code P}
 *       lives, without any coordination round-trip.</li>
 *   <li>A source reading partition {@code P} knows exactly one worker to
 *       {@code DoGet} from.</li>
 *   <li>Load across workers is uniform when Trino's partitioning function
 *       is itself balanced (hash join keys, etc.).</li>
 * </ul>
 *
 * <p>Storage key format on the worker: {@code "exchange/{exchangeId}/{partitionId}"}.
 * Using the exchange id as a prefix keeps Prism's in-memory partition store
 * (see {@code ShuffleFlightService.PartitionStore}) free of key collisions
 * across concurrent queries and across stages within a query.</p>
 */
public final class PrismPartitionRouter {

    private final int workerCount;

    public PrismPartitionRouter(int workerCount) {
        if (workerCount < 1) {
            throw new IllegalArgumentException("workerCount must be >= 1, got " + workerCount);
        }
        this.workerCount = workerCount;
    }

    public int workerFor(int partitionId) {
        // Math.floorMod handles negative partition ids defensively; Trino
        // never emits them but it's cheap insurance.
        return Math.floorMod(partitionId, workerCount);
    }

    public static String storageKey(String exchangeId, int partitionId) {
        return "exchange/" + exchangeId + "/" + partitionId;
    }

    public int workerCount() {
        return workerCount;
    }
}
