package io.prism.plugin;

import io.prism.bridge.PrismFlightExecutor;
import io.prism.plugin.events.PrismQueryStats;
import io.prism.plugin.events.PrismQueryStatsRegistry;
import io.trino.spi.connector.*;

import java.util.List;

public class PrismPageSourceProvider implements ConnectorPageSourceProvider {
    private final PrismFlightExecutor executor;

    public PrismPageSourceProvider(PrismFlightExecutor executor) {
        this.executor = executor;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {
        PrismSplit prismSplit = (PrismSplit) split;
        PrismTableHandle prismTable = (PrismTableHandle) table;

        List<PrismColumnHandle> prismColumns = columns.stream()
                .map(c -> (PrismColumnHandle) c)
                .toList();

        // Associate this page source with a per-query stats bucket so that the
        // PrismEventListener can emit Prism-specific fields on query completion,
        // keyed by the Trino query ID. The bucket is created lazily here and
        // removed by the listener on queryCompleted.
        PrismQueryStats stats = PrismQueryStatsRegistry.getInstance()
                .getOrCreate(session.getQueryId());

        return new PrismPageSource(executor, prismSplit, prismTable, prismColumns, stats);
    }
}
