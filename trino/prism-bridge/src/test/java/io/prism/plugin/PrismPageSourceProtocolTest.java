package io.prism.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.trino.spi.security.SelectedRole;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    @Test
    public void putSessionContext_emitsIdentityGroupsRolesAndCredentials() {
        ObjectNode command = M.createObjectNode();
        TestSession session = TestSession.withDefaults()
                .withGroups(Set.of("finance", "pii"))
                .withEnabledRoles(Set.of("analyst"))
                .withConnectorRole(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("catalog_role")))
                .withExtraCredentials(Map.of("region", "us", "token", "secret"));

        PrismPageSource.putSessionContext(command, session);

        ObjectNode sessionContext = (ObjectNode) command.get("session_context");
        assertNotNull(sessionContext);
        assertEquals("test", sessionContext.get("user").asText());
        assertEquals(Set.of("finance", "pii"),
                Set.copyOf(M.convertValue(sessionContext.get("groups"), java.util.List.class)));
        var roles = M.convertValue(sessionContext.get("roles"), java.util.List.class);
        assertTrue(roles.contains("analyst"));
        assertTrue(roles.stream().anyMatch(role -> role.toString().contains("catalog_role")));
        assertEquals("us", sessionContext.get("extra_credentials").get("region").asText());
        assertEquals("secret", sessionContext.get("extra_credentials").get("token").asText());
    }
}
