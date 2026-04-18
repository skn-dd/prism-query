package io.prism.plugin;

import io.prism.bridge.PrismFlightExecutor;
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

        return new PrismPageSource(executor, prismSplit, prismTable, prismColumns, session);
    }
}
