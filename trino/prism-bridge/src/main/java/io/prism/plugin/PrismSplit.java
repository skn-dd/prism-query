package io.prism.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;

public final class PrismSplit implements ConnectorSplit {
    private final int workerIndex;
    private final String workerEndpoint;

    @JsonCreator
    public PrismSplit(
            @JsonProperty("workerIndex") int workerIndex,
            @JsonProperty("workerEndpoint") String workerEndpoint) {
        this.workerIndex = workerIndex;
        this.workerEndpoint = workerEndpoint;
    }

    @JsonProperty
    public int getWorkerIndex() { return workerIndex; }

    @JsonProperty
    public String getWorkerEndpoint() { return workerEndpoint; }

    @Override
    public Map<String, String> getSplitInfo() {
        return Map.of("workerIndex", String.valueOf(workerIndex), "endpoint", workerEndpoint);
    }

    @Override
    public long getRetainedSizeInBytes() {
        return 0;
    }
}
