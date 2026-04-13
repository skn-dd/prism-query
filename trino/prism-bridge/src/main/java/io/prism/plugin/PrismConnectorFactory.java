package io.prism.plugin;

import io.prism.bridge.PrismFlightExecutor;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PrismConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "prism";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        String workersStr = config.getOrDefault("prism.workers", "localhost:50051");
        List<String> workers = Arrays.stream(workersStr.split(","))
                .map(String::trim)
                .collect(Collectors.toList());

        PrismFlightExecutor executor = new PrismFlightExecutor(workers);
        return new PrismConnector(executor);
    }
}
