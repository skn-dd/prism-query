package io.prism.bridge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.file.Path;

/**
 * JNI bridge to the Prism native executor.
 *
 * <p>Manages the lifecycle of the native Rust runtime and provides
 * methods to execute Substrait plans on Arrow data. Each Trino worker
 * creates one instance of this class at startup.</p>
 *
 * <p>The native library ({@code libprism_jni.so} / {@code libprism_jni.dylib})
 * must be on {@code java.library.path} or loaded explicitly.</p>
 */
public class PrismNativeExecutor implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PrismNativeExecutor.class);

    private final long runtimeHandle;
    private volatile boolean closed = false;

    // --- Native methods ---

    private static native long init();
    private static native void shutdown(long runtimeHandle);
    private static native byte[] executePlan(long runtimeHandle, byte[] substraitPlan, byte[] inputData);
    private static native String version();

    // --- Library loading ---

    static {
        try {
            String libPath = System.getProperty("prism.native.lib.path");
            if (libPath != null) {
                System.load(Path.of(libPath).toAbsolutePath().toString());
            } else {
                System.loadLibrary("prism_jni");
            }
            LOG.info("Prism native library loaded: {}", version());
        } catch (UnsatisfiedLinkError e) {
            LOG.error("Failed to load Prism native library. "
                    + "Set -Dprism.native.lib.path=<path> or add to java.library.path", e);
            throw e;
        }
    }

    /**
     * Create a new PrismNativeExecutor, initializing the native runtime.
     */
    public PrismNativeExecutor() {
        this.runtimeHandle = init();
        LOG.info("Prism native executor initialized (handle={})", runtimeHandle);
    }

    /**
     * Execute a Substrait plan against Arrow IPC input data.
     *
     * @param substraitPlan Substrait plan serialized as protobuf bytes
     * @param inputData     Input RecordBatches serialized as Arrow IPC stream
     * @return Output RecordBatches serialized as Arrow IPC stream
     * @throws PrismExecutionException if native execution fails
     */
    public byte[] execute(byte[] substraitPlan, byte[] inputData) {
        if (closed) {
            throw new IllegalStateException("PrismNativeExecutor has been shut down");
        }
        try {
            return executePlan(runtimeHandle, substraitPlan, inputData);
        } catch (RuntimeException e) {
            throw new PrismExecutionException("Native execution failed", e);
        }
    }

    /**
     * Get the version of the native executor library.
     */
    public static String nativeVersion() {
        return version();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            shutdown(runtimeHandle);
            LOG.info("Prism native executor shut down");
        }
    }
}
