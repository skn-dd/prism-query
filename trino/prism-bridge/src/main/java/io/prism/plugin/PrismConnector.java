package io.prism.plugin;

import io.prism.bridge.PrismFlightExecutor;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

public class PrismConnector implements Connector {
    private final PrismFlightExecutor executor;
    private final PrismMetadata metadata;
    private final PrismSplitManager splitManager;
    private final PrismPageSourceProvider pageSourceProvider;

    public PrismConnector(PrismFlightExecutor executor) {
        this.executor = executor;
        this.metadata = new PrismMetadata();
        this.splitManager = new PrismSplitManager(executor);
        this.pageSourceProvider = new PrismPageSourceProvider(executor);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return PrismTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return pageSourceProvider;
    }

    @Override
    public void shutdown() {
        executor.close();
    }
}
