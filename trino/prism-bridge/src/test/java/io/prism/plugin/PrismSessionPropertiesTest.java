package io.prism.plugin;

import io.trino.spi.session.PropertyMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class PrismSessionPropertiesTest {

    @Test
    void registrationReturnsExpectedShape() {
        PrismSessionProperties props = new PrismSessionProperties();
        List<PropertyMetadata<?>> registry = props.getSessionProperties();

        Set<String> names = registry.stream()
                .map(PropertyMetadata::getName)
                .collect(Collectors.toSet());

        assertEquals(
                Set.of(
                        PrismSessionProperties.ACCELERATION_ENABLED,
                        PrismSessionProperties.MEMORY_BUDGET_GB,
                        PrismSessionProperties.MAX_FILES_PER_SPLIT,
                        PrismSessionProperties.PREFER_SINGLE_WORKER),
                names,
                "registered property names should match the Wave 2 design");

        // Every property must carry a non-empty description — these surface to DBAs.
        for (PropertyMetadata<?> p : registry) {
            assertNotNull(p.getDescription(), "description missing for " + p.getName());
            assertFalse(p.getDescription().isBlank(), "blank description for " + p.getName());
            assertFalse(p.isHidden(), p.getName() + " should be visible to users");
        }
    }

    @Test
    void defaultsMatchDesign() {
        PrismSessionProperties props = new PrismSessionProperties();
        Map<String, Object> defaults = props.getSessionProperties().stream()
                .collect(Collectors.toMap(PropertyMetadata::getName, PropertyMetadata::getDefaultValue));

        assertEquals(Boolean.TRUE, defaults.get(PrismSessionProperties.ACCELERATION_ENABLED));
        assertEquals(0, defaults.get(PrismSessionProperties.MEMORY_BUDGET_GB));
        assertEquals(64, defaults.get(PrismSessionProperties.MAX_FILES_PER_SPLIT));
        assertEquals(Boolean.FALSE, defaults.get(PrismSessionProperties.PREFER_SINGLE_WORKER));
    }

    @Test
    void typedAccessorsReflectSessionState() {
        TestSession baseline = TestSession.withDefaults();
        assertTrue(PrismSessionProperties.isAccelerationEnabled(baseline));
        assertEquals(0, PrismSessionProperties.getMemoryBudgetGb(baseline));
        assertEquals(64, PrismSessionProperties.getMaxFilesPerSplit(baseline));
        assertFalse(PrismSessionProperties.isPreferSingleWorker(baseline));

        TestSession overridden = baseline
                .with(PrismSessionProperties.ACCELERATION_ENABLED, false)
                .with(PrismSessionProperties.MEMORY_BUDGET_GB, 16)
                .with(PrismSessionProperties.MAX_FILES_PER_SPLIT, 128)
                .with(PrismSessionProperties.PREFER_SINGLE_WORKER, true);

        assertFalse(PrismSessionProperties.isAccelerationEnabled(overridden));
        assertEquals(16, PrismSessionProperties.getMemoryBudgetGb(overridden));
        assertEquals(128, PrismSessionProperties.getMaxFilesPerSplit(overridden));
        assertTrue(PrismSessionProperties.isPreferSingleWorker(overridden));
    }

    @Test
    void catalogConfigSetsDefaults_bareNames() {
        PrismSessionProperties props = PrismSessionProperties.fromCatalogConfig(Map.of(
                "acceleration_enabled", "false",
                "memory_budget_gb", "8",
                "max_files_per_split", "32",
                "prefer_single_worker", "true"));

        Map<String, Object> defaults = props.getSessionProperties().stream()
                .collect(Collectors.toMap(PropertyMetadata::getName, PropertyMetadata::getDefaultValue));

        assertEquals(Boolean.FALSE, defaults.get(PrismSessionProperties.ACCELERATION_ENABLED));
        assertEquals(8, defaults.get(PrismSessionProperties.MEMORY_BUDGET_GB));
        assertEquals(32, defaults.get(PrismSessionProperties.MAX_FILES_PER_SPLIT));
        assertEquals(Boolean.TRUE, defaults.get(PrismSessionProperties.PREFER_SINGLE_WORKER));
    }

    @Test
    void catalogConfigSetsDefaults_prismPrefix() {
        // `prism.acceleration_enabled` prefix form is also accepted.
        PrismSessionProperties props = PrismSessionProperties.fromCatalogConfig(Map.of(
                "prism.acceleration_enabled", "false",
                "prism.memory_budget_gb", "4"));

        Map<String, Object> defaults = props.getSessionProperties().stream()
                .collect(Collectors.toMap(PropertyMetadata::getName, PropertyMetadata::getDefaultValue));

        assertEquals(Boolean.FALSE, defaults.get(PrismSessionProperties.ACCELERATION_ENABLED));
        assertEquals(4, defaults.get(PrismSessionProperties.MEMORY_BUDGET_GB));
        // Untouched properties retain their code defaults.
        assertEquals(64, defaults.get(PrismSessionProperties.MAX_FILES_PER_SPLIT));
    }

    @Test
    void catalogConfigRejectsMalformedInteger() {
        assertThrows(IllegalArgumentException.class,
                () -> PrismSessionProperties.fromCatalogConfig(Map.of("memory_budget_gb", "not-a-number")));
    }

    @Test
    void sessionOverrideBeatsCatalogDefault() {
        // Catalog defaults acceleration to false, session flips it back to true.
        List<PropertyMetadata<?>> registry = PrismSessionProperties.fromCatalogConfig(
                        Map.of("acceleration_enabled", "false"))
                .getSessionProperties();
        TestSession session = TestSession.from(registry)
                .with(PrismSessionProperties.ACCELERATION_ENABLED, true);

        assertTrue(PrismSessionProperties.isAccelerationEnabled(session),
                "per-session SET SESSION must override catalog-level default");
    }
}
