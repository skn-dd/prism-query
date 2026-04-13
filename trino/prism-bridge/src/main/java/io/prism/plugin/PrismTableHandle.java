package io.prism.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Objects;
import java.util.Optional;

public final class PrismTableHandle implements ConnectorTableHandle {
    private final String schemaName;
    private final String tableName;
    private final PrismPlanNode pushedPlan; // nullable

    @JsonCreator
    public PrismTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("pushedPlan") PrismPlanNode pushedPlan) {
        this.schemaName = Objects.requireNonNull(schemaName);
        this.tableName = Objects.requireNonNull(tableName);
        this.pushedPlan = pushedPlan;
    }

    public PrismTableHandle(String schemaName, String tableName) {
        this(schemaName, tableName, null);
    }

    @JsonProperty
    public String getSchemaName() { return schemaName; }

    @JsonProperty
    public String getTableName() { return tableName; }

    @JsonProperty("pushedPlan")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public PrismPlanNode getPushedPlanJson() { return pushedPlan; }

    public Optional<PrismPlanNode> getPushedPlan() {
        return Optional.ofNullable(pushedPlan);
    }

    public PrismTableHandle withPushedPlan(PrismPlanNode plan) {
        return new PrismTableHandle(schemaName, tableName, plan);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PrismTableHandle that)) return false;
        return schemaName.equals(that.schemaName) && tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName, tableName);
    }

    @Override
    public String toString() {
        return schemaName + "." + tableName + (pushedPlan != null ? "[pushed]" : "");
    }
}
