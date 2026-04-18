package io.prism.bridge;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PrismFlightExecutorTest {

    @Test
    void malformedEndpointRaisesConfigurationInvalid() {
        TrinoException te = assertThrows(TrinoException.class,
                () -> new PrismFlightExecutor(List.of("not-a-host-port")));
        assertEquals(StandardErrorCode.CONFIGURATION_INVALID.toErrorCode(), te.getErrorCode());
    }

    @Test
    void nonNumericPortRaisesConfigurationInvalid() {
        TrinoException te = assertThrows(TrinoException.class,
                () -> new PrismFlightExecutor(List.of("host:not-a-port")));
        assertEquals(StandardErrorCode.CONFIGURATION_INVALID.toErrorCode(), te.getErrorCode());
    }
}
