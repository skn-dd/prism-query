package io.prism.bridge;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;

import java.io.IOException;
import java.util.Optional;

/**
 * Maps Prism-bridge internal failures (Arrow Flight, Arrow IPC, JNI) onto
 * Trino's {@link StandardErrorCode} vocabulary.
 *
 * <p>This is the single choke-point where connector-internal exceptions
 * are turned into {@link TrinoException} instances. Call sites that cross
 * the Trino-facing boundary (e.g. {@code PrismPageSource}, {@code PrismFlightExecutor})
 * must funnel through here so that failures surface with meaningful error codes
 * rather than opaque {@code GENERIC_INTERNAL_ERROR}.</p>
 *
 * <p>Classification rules — keep in sync with the design doc:</p>
 * <ul>
 *   <li>Flight {@code UNAVAILABLE} / {@code CANCELLED} → {@link StandardErrorCode#REMOTE_HOST_GONE}</li>
 *   <li>Flight {@code TIMED_OUT} → {@link StandardErrorCode#REMOTE_TASK_ERROR}</li>
 *   <li>Flight {@code RESOURCE_EXHAUSTED} → {@link StandardErrorCode#EXCEEDED_LOCAL_MEMORY_LIMIT}</li>
 *   <li>Flight {@code UNIMPLEMENTED} / {@code INVALID_ARGUMENT} → {@link StandardErrorCode#NOT_SUPPORTED}</li>
 *   <li>Flight {@code UNAUTHENTICATED} / {@code UNAUTHORIZED} → {@link StandardErrorCode#CONFIGURATION_INVALID}</li>
 *   <li>Everything else (INTERNAL, UNKNOWN, NOT_FOUND, IOException, generic) →
 *       {@link StandardErrorCode#GENERIC_INTERNAL_ERROR}</li>
 * </ul>
 */
public final class PrismErrorMapper {

    private PrismErrorMapper() {}

    /**
     * Wrap a failure from a specific worker into a {@link TrinoException}.
     *
     * @param workerAddress host:port of the worker whose call failed, or null if unknown
     * @param context       short descriptor of the operation (e.g. "executeQuery", "fetchResultStream")
     * @param cause         the underlying exception
     * @return a TrinoException with the classified error code and a meaningful message
     */
    public static TrinoException wrap(String workerAddress, String context, Throwable cause) {
        StandardErrorCode code = classify(cause);
        String message = buildMessage(workerAddress, context, cause);
        return new TrinoException(code, message, cause);
    }

    /**
     * Wrap a failure with no associated worker (e.g. Substrait serialization).
     */
    public static TrinoException wrap(String context, Throwable cause) {
        return wrap(null, context, cause);
    }

    /**
     * Build a TrinoException for a well-classified failure where the cause
     * is already known to map cleanly onto a specific code.
     */
    public static TrinoException forCode(StandardErrorCode code, String workerAddress, String context, Throwable cause) {
        return new TrinoException(code, buildMessage(workerAddress, context, cause), cause);
    }

    /**
     * Build a TrinoException with no underlying cause (pure message error).
     */
    public static TrinoException forCode(StandardErrorCode code, String message) {
        return new TrinoException(code, message);
    }

    /**
     * Classify an arbitrary Throwable into a StandardErrorCode.
     * Public so callers can peek at classification without building the full message.
     */
    public static StandardErrorCode classify(Throwable cause) {
        FlightRuntimeException fre = unwrapFlight(cause);
        if (fre != null) {
            FlightStatusCode status = fre.status().code();
            return switch (status) {
                case UNAVAILABLE, CANCELLED -> StandardErrorCode.REMOTE_HOST_GONE;
                case TIMED_OUT -> StandardErrorCode.REMOTE_TASK_ERROR;
                case RESOURCE_EXHAUSTED -> StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
                case UNIMPLEMENTED, INVALID_ARGUMENT -> StandardErrorCode.NOT_SUPPORTED;
                case UNAUTHENTICATED, UNAUTHORIZED -> StandardErrorCode.CONFIGURATION_INVALID;
                default -> StandardErrorCode.GENERIC_INTERNAL_ERROR;
            };
        }
        if (cause instanceof IOException) {
            return StandardErrorCode.GENERIC_INTERNAL_ERROR;
        }
        return StandardErrorCode.GENERIC_INTERNAL_ERROR;
    }

    private static FlightRuntimeException unwrapFlight(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof FlightRuntimeException fre) {
                return fre;
            }
            cur = cur.getCause();
            if (cur == t) { // paranoid cycle guard
                return null;
            }
        }
        return null;
    }

    private static String buildMessage(String workerAddress, String context, Throwable cause) {
        StringBuilder sb = new StringBuilder("Prism ");
        sb.append(context);
        sb.append(" failed");
        if (workerAddress != null && !workerAddress.isEmpty()) {
            sb.append(" [worker=").append(workerAddress).append(']');
        }
        String causeMsg = Optional.ofNullable(cause).map(Throwable::getMessage).orElse(null);
        if (causeMsg != null && !causeMsg.isBlank()) {
            sb.append(": ").append(causeMsg);
        }
        return sb.toString();
    }
}
