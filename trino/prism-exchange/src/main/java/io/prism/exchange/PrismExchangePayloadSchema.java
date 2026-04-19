package io.prism.exchange;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Fixed Arrow schema used by the Prism exchange transport.
 *
 * <p>Trino hands the exchange sink opaque {@link io.airlift.slice.Slice}
 * payloads (serialized Trino pages). Those bytes are not Arrow record
 * batches — they're Trino's own serialization format. We wrap them in a
 * single-column binary Arrow IPC stream so we can reuse the existing
 * Prism Flight server's {@code do_put} / {@code do_get} path without
 * adding a new Flight action.</p>
 *
 * <p>Schema:</p>
 * <pre>
 * exchange_payload: Table
 *   payload: LargeBinary (nullable=false)
 * </pre>
 *
 * <p>{@code LargeBinary} is used instead of {@code Binary} because a single
 * Trino page Slice can exceed the 2 GiB limit of the 32-bit offset variant.
 * Each Arrow batch carries exactly one row per {@code add()} call.</p>
 */
public final class PrismExchangePayloadSchema {

    public static final String PAYLOAD_FIELD = "payload";

    private static final Schema SCHEMA = new Schema(List.of(
            new Field(PAYLOAD_FIELD,
                    new FieldType(/*nullable=*/ false, new ArrowType.LargeBinary(), /*dictionary=*/ null),
                    /*children=*/ List.of())));

    private PrismExchangePayloadSchema() {}

    public static Schema schema() {
        return SCHEMA;
    }
}
