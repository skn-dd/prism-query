package io.prism.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the v2 `tables` protocol wire shape emitted by
 * {@link PrismPageSource#putTableSpec}.
 *
 * Protocol v2 contract (Wave 2a slice 1):
 * <pre>
 * "tables": {
 *   "&lt;name&gt;": {
 *     "uris": ["s3://...", ...],
 *     "store_key": "&lt;optional-legacy-fallback&gt;"
 *   },
 *   ...
 * }
 * </pre>
 * Today the bench path emits empty {@code uris}; metadata delegation will
 * populate that array in Wave 2b.
 */
public class PrismPageSourceProtocolTest {
    private static final ObjectMapper M = new ObjectMapper();

    @Test
    public void putTableSpec_emitsV2Shape() {
        ObjectNode tables = M.createObjectNode();
        PrismPageSource.putTableSpec(tables, "lineitem", "tpch/lineitem");

        assertTrue(tables.has("lineitem"), "lineitem key must exist");
        var spec = tables.get("lineitem");
        assertTrue(spec.isObject(), "spec must be a JSON object, not a bare string");

        var uris = spec.get("uris");
        assertNotNull(uris, "uris field must be present");
        assertTrue(uris.isArray(), "uris must be an array");
        assertEquals(0, ((ArrayNode) uris).size(),
                "bench path emits empty uris (delegation populates in Wave 2b)");

        var storeKey = spec.get("store_key");
        assertNotNull(storeKey, "store_key must be present on bench path");
        assertEquals("tpch/lineitem", storeKey.asText());
    }

    @Test
    public void putTableSpec_broadcastSuffixIsPreserved() {
        ObjectNode tables = M.createObjectNode();
        PrismPageSource.putTableSpec(tables, "orders", "tpch/orders_broadcast");

        var spec = tables.get("orders");
        assertEquals("tpch/orders_broadcast", spec.get("store_key").asText(),
                "broadcast-suffixed store_key must round-trip intact");
    }

    @Test
    public void putTableSpec_multipleTablesCoexist() {
        ObjectNode tables = M.createObjectNode();
        PrismPageSource.putTableSpec(tables, "lineitem", "tpch/lineitem");
        PrismPageSource.putTableSpec(tables, "orders", "tpch/orders_broadcast");

        assertTrue(tables.get("lineitem").isObject());
        assertTrue(tables.get("orders").isObject());
        assertEquals(2, tables.size());
    }
}
