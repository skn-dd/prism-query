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

        PrismQueryStats stats = PrismQueryStatsRegistry.getInstance()
                .getOrCreate(session.getQueryId());

        return new PrismPageSource(executor, prismSplit, prismTable, prismColumns, session, stats);
    }
}
