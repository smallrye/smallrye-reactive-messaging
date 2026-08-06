package io.smallrye.reactive.messaging.connector;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Lifecycle hooks for connectors, called per-channel during shutdown.
 * <p>
 * The per-channel shutdown sequence is:
 * <ol>
 * <li>Drain in-flight messages (framework-managed via pausable channel)</li>
 * <li>{@link #preShutdownIncoming(String)} / {@link #preShutdownOutgoing(String)} — connector-specific cleanup
 * while the transport client is still alive (e.g. flush commits, terminate failure handlers)</li>
 * <li>{@link #shutdownIncoming(String)} / {@link #shutdownOutgoing(String)} — close the transport client
 * and release resources</li>
 * </ol>
 * <p>
 * After all channels have been shut down, {@link #terminate()} is called once per connector
 * to clean up connector-wide resources (e.g. admin clients, caches, shared connections).
 */
public interface ConnectorLifecycle {

    /**
     * Called after in-flight messages have been drained but before the stream is cancelled.
     * Use this to perform connector-specific cleanup that requires the transport client to be alive.
     *
     * @param channel the channel name
     * @return a {@link CompletionStage} completed when pre-shutdown is done
     */
    default CompletionStage<Void> preShutdownIncoming(String channel) {
        return CompletableFuture.completedStage(null);
    }

    /**
     * Called after in-flight messages have been drained but before the stream is cancelled.
     * Use this to perform connector-specific cleanup that requires the transport client to be alive.
     *
     * @param channel the channel name
     * @return a {@link CompletionStage} completed when pre-shutdown is done
     */
    default CompletionStage<Void> preShutdownOutgoing(String channel) {
        return CompletableFuture.completedStage(null);
    }

    /**
     * Called after the stream subscription has been cancelled.
     * Use this to close the transport client and release resources.
     *
     * @param channel the channel name
     * @return a {@link CompletionStage} completed when shutdown is done
     */
    default CompletionStage<Void> shutdownIncoming(String channel) {
        return CompletableFuture.completedStage(null);
    }

    /**
     * Called after the stream subscription has been cancelled.
     * Use this to close the transport client and release resources.
     *
     * @param channel the channel name
     * @return a {@link CompletionStage} completed when shutdown is done
     */
    default CompletionStage<Void> shutdownOutgoing(String channel) {
        return CompletableFuture.completedStage(null);
    }

    /**
     * Called once after all channels for this connector have been shut down.
     * Use this to clean up connector-wide resources such as admin clients, caches, or shared connections.
     *
     * @return a {@link CompletionStage} completed when connector-wide cleanup is done
     */
    default CompletionStage<Void> terminate() {
        return CompletableFuture.completedStage(null);
    }
}
