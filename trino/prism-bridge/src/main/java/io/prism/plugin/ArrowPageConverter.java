package io.prism.plugin;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.*;
import io.trino.spi.type.*;
import org.apache.arrow.vector.*;

import java.util.List;

/**
 * Converts Arrow VectorSchemaRoot to Trino Page.
 * Only used for final results (post-agg/post-limit), so typically small.
 */
public final class ArrowPageConverter {

    private ArrowPageConverter() {}

    public static Page toPage(VectorSchemaRoot root, List<PrismColumnHandle> requestedColumns) {
        int rowCount = root.getRowCount();
        if (rowCount == 0) {
            return null;
        }

        // If no columns requested (e.g. COUNT(*)), return a page with just the row count
        if (requestedColumns.isEmpty()) {
            return new Page(rowCount);
        }

        // Map Arrow vectors to requested columns by name for robustness
        Block[] blocks = new Block[requestedColumns.size()];
        for (int i = 0; i < requestedColumns.size(); i++) {
            String colName = requestedColumns.get(i).getColumnName();
            FieldVector vector = null;

            // Try to find by name first
            for (FieldVector fv : root.getFieldVectors()) {
                if (fv.getName().equals(colName)) {
                    vector = fv;
                    break;
                }
            }

            // Fall back to positional if name not found
            if (vector == null && i < root.getFieldVectors().size()) {
                vector = root.getFieldVectors().get(i);
            }

            if (vector == null) {
                throw new RuntimeException("Column '" + colName + "' not found in Arrow result");
            }

            blocks[i] = convertVector(vector, rowCount);
        }

        return new Page(rowCount, blocks);
    }

    private static Block convertVector(FieldVector vector, int rowCount) {
        if (vector instanceof BigIntVector bigIntVector) {
            return convertBigInt(bigIntVector, rowCount);
        } else if (vector instanceof IntVector intVector) {
            return convertInt(intVector, rowCount);
        } else if (vector instanceof Float8Vector float8Vector) {
            return convertDouble(float8Vector, rowCount);
        } else if (vector instanceof Float4Vector float4Vector) {
            return convertFloat(float4Vector, rowCount);
        } else if (vector instanceof VarCharVector varCharVector) {
            return convertVarChar(varCharVector, rowCount);
        } else if (vector instanceof BitVector bitVector) {
            return convertBoolean(bitVector, rowCount);
        } else {
            // Fallback: treat as varchar
            return convertToString(vector, rowCount);
        }
    }

    private static Block convertBigInt(BigIntVector vector, int rowCount) {
        BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, rowCount);
        for (int i = 0; i < rowCount; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            } else {
                BigintType.BIGINT.writeLong(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    private static Block convertInt(IntVector vector, int rowCount) {
        BlockBuilder builder = IntegerType.INTEGER.createBlockBuilder(null, rowCount);
        for (int i = 0; i < rowCount; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            } else {
                IntegerType.INTEGER.writeLong(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    private static Block convertDouble(Float8Vector vector, int rowCount) {
        BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, rowCount);
        for (int i = 0; i < rowCount; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            } else {
                DoubleType.DOUBLE.writeDouble(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    private static Block convertFloat(Float4Vector vector, int rowCount) {
        BlockBuilder builder = RealType.REAL.createBlockBuilder(null, rowCount);
        for (int i = 0; i < rowCount; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            } else {
                RealType.REAL.writeLong(builder, Float.floatToIntBits(vector.get(i)));
            }
        }
        return builder.build();
    }

    private static Block convertVarChar(VarCharVector vector, int rowCount) {
        BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, rowCount);
        for (int i = 0; i < rowCount; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            } else {
                byte[] bytes = vector.get(i);
                VarcharType.VARCHAR.writeSlice(builder, Slices.wrappedBuffer(bytes));
            }
        }
        return builder.build();
    }

    private static Block convertBoolean(BitVector vector, int rowCount) {
        BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, rowCount);
        for (int i = 0; i < rowCount; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            } else {
                BooleanType.BOOLEAN.writeBoolean(builder, vector.get(i) != 0);
            }
        }
        return builder.build();
    }

    private static Block convertToString(FieldVector vector, int rowCount) {
        BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, rowCount);
        for (int i = 0; i < rowCount; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            } else {
                Object val = vector.getObject(i);
                byte[] bytes = val.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                VarcharType.VARCHAR.writeSlice(builder, Slices.wrappedBuffer(bytes));
            }
        }
        return builder.build();
    }
}
