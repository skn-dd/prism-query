package io.prism.plugin;

import io.prism.bridge.PrismFlightExecutor;
import io.trino.spi.connector.*;

import java.util.ArrayList;
import java.util.List;

public class PrismSplitManager implements ConnectorSplitManager {
    private final PrismFlightExecutor executor;

    public PrismSplitManager(PrismFlightExecutor executor) {
        this.executor = executor;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        PrismTableHandle prismTable = (PrismTableHandle) table;
        int workerCount = executor.workerCount();
        if (workerCount == 0) {
            workerCount = 1;
        }

        // Use a single split — both workers have identical data (full SF=1 dataset each).
        // A single split avoids double-counting and reduces unnecessary network transfer.
        // Future: partition data across workers and use workerCount splits.
        List<ConnectorSplit> splits = List.of(new PrismSplit(0, "localhost:50051"));
        return new FixedSplitSource(splits);
    }
}
