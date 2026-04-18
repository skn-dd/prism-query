package io.prism.bridge;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that internal Prism failures are mapped onto appropriate
 * Trino {@link StandardErrorCode} values. This is the Wave 2a slice 4
 * error-surface contract.
 */
class PrismErrorMapperTest {

    private static FlightRuntimeException flight(FlightStatusCode code, String msg) {
        return new CallStatus(code).withDescription(msg).toRuntimeException();
    }

    // ---- classify() ----

    @Test
    void flightUnavailableMapsToRemoteHostGone() {
        assertEquals(StandardErrorCode.REMOTE_HOST_GONE,
                PrismErrorMapper.classify(flight(FlightStatusCode.UNAVAILABLE, "worker down")));
    }

    @Test
    void flightCancelledMapsToRemoteHostGone() {
        assertEquals(StandardErrorCode.REMOTE_HOST_GONE,
                PrismErrorMapper.classify(flight(FlightStatusCode.CANCELLED, "cancelled")));
    }

    @Test
    void flightTimedOutMapsToRemoteTaskError() {
        assertEquals(StandardErrorCode.REMOTE_TASK_ERROR,
                PrismErrorMapper.classify(flight(FlightStatusCode.TIMED_OUT, "timeout")));
    }

    @Test
    void flightResourceExhaustedMapsToLocalMemoryLimit() {
        assertEquals(StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT,
                PrismErrorMapper.classify(flight(FlightStatusCode.RESOURCE_EXHAUSTED, "oom")));
    }

    @Test
    void flightUnimplementedMapsToNotSupported() {
        assertEquals(StandardErrorCode.NOT_SUPPORTED,
                PrismErrorMapper.classify(flight(FlightStatusCode.UNIMPLEMENTED, "op not supported")));
    }

    @Test
    void flightInvalidArgumentMapsToNotSupported() {
        assertEquals(StandardErrorCode.NOT_SUPPORTED,
                PrismErrorMapper.classify(flight(FlightStatusCode.INVALID_ARGUMENT, "bad plan")));
    }

    @Test
    void flightUnauthenticatedMapsToConfigurationInvalid() {
        assertEquals(StandardErrorCode.CONFIGURATION_INVALID,
                PrismErrorMapper.classify(flight(FlightStatusCode.UNAUTHENTICATED, "missing creds")));
    }

    @Test
    void flightInternalMapsToGenericInternalError() {
        assertEquals(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                PrismErrorMapper.classify(flight(FlightStatusCode.INTERNAL, "boom")));
    }

    @Test
    void nestedFlightExceptionIsUnwrapped() {
        RuntimeException outer = new RuntimeException("outer",
                flight(FlightStatusCode.UNAVAILABLE, "worker down"));
        assertEquals(StandardErrorCode.REMOTE_HOST_GONE, PrismErrorMapper.classify(outer));
    }

    @Test
    void ioExceptionMapsToGenericInternalError() {
        assertEquals(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                PrismErrorMapper.classify(new IOException("arrow ipc malformed")));
    }

    @Test
    void plainRuntimeExceptionMapsToGenericInternalError() {
        assertEquals(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                PrismErrorMapper.classify(new RuntimeException("something")));
    }

    // ---- wrap() ----

    @Test
    void wrapProducesTrinoExceptionWithClassifiedCode() {
        TrinoException te = PrismErrorMapper.wrap(
                "worker-a:7777",
                "executeQuery",
                flight(FlightStatusCode.UNAVAILABLE, "worker crashed"));
        assertEquals(StandardErrorCode.REMOTE_HOST_GONE.toErrorCode(), te.getErrorCode());
        assertTrue(te.getMessage().contains("worker-a:7777"),
                "Message should include worker address: " + te.getMessage());
        assertTrue(te.getMessage().contains("executeQuery"),
                "Message should include operation context: " + te.getMessage());
    }

    @Test
    void wrapPreservesCauseChain() {
        FlightRuntimeException cause = flight(FlightStatusCode.INTERNAL, "nope");
        TrinoException te = PrismErrorMapper.wrap("w:1", "fetch", cause);
        assertSame(cause, te.getCause());
    }

    @Test
    void wrapWithoutWorkerAddressOmitsAddressClause() {
        TrinoException te = PrismErrorMapper.wrap("serialize", new RuntimeException("bad"));
        assertFalse(te.getMessage().contains("[worker="),
                "Message should omit worker clause when address is null: " + te.getMessage());
    }

    @Test
    void forCodeWithMessageOnlyWorks() {
        TrinoException te = PrismErrorMapper.forCode(
                StandardErrorCode.CONFIGURATION_INVALID, "bad endpoint");
        assertEquals(StandardErrorCode.CONFIGURATION_INVALID.toErrorCode(), te.getErrorCode());
        assertTrue(te.getMessage().contains("bad endpoint"));
    }
}
