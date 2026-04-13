package io.prism.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Objects;

public final class PrismColumnHandle implements ColumnHandle {
    private final String columnName;
    private final int columnIndex;
    private final Type type;

    @JsonCreator
    public PrismColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnIndex") int columnIndex,
            @JsonProperty("type") Type type) {
        this.columnName = Objects.requireNonNull(columnName);
        this.columnIndex = columnIndex;
        this.type = Objects.requireNonNull(type);
    }

    @JsonProperty
    public String getColumnName() { return columnName; }

    @JsonProperty
    public int getColumnIndex() { return columnIndex; }

    @JsonProperty
    public Type getType() { return type; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PrismColumnHandle that)) return false;
        return columnIndex == that.columnIndex && columnName.equals(that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, columnIndex);
    }

    @Override
    public String toString() {
        return columnName + ":" + type;
    }
}
