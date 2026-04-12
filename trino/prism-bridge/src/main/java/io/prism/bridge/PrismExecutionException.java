package io.prism.bridge;

/**
 * Exception thrown when native Prism execution fails.
 */
public class PrismExecutionException extends RuntimeException {

    public PrismExecutionException(String message) {
        super(message);
    }

    public PrismExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
