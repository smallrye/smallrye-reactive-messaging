package io.smallrye.reactive.messaging.connector;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Lifecycle hooks for connectors, called per-channel during close.
 * <p>
 * The per-channel hooks are called in two separate phases:
 * <ol>
 * <li><strong>Prepare-close phase</strong> — the framework drains in-flight messages (via pausable channel),
 * then calls {@link #prepareCloseIncoming(String)} / {@link #prepareCloseOutgoing(String)} for
 * connector-specific cleanup while the transport client is still alive
 * (e.g. flush commits, terminate failure handlers).</li>
 * <li><strong>Close phase</strong> (later, during container teardown) — calls
 * {@link #closeIncoming(String)} / {@link #closeOutgoing(String)} to close the transport client
 * and release resources.</li>
 * </ol>
 * <p>
 * After all channels have been closed, {@link #terminate()} is called once per connector
 * to clean up connector-wide resources (e.g. admin clients, caches, shared connections).
 * <p>
 * <strong>Idempotency:</strong> {@link #terminate()} acts as a safety net in environments that do not use the
 * {@code ChannelLifecycleManager} (e.g. WildFly). It must be able to close all channels independently.
 * When the lifecycle manager is present, per-channel hooks are called first, and {@code terminate()} runs
 * afterward. Implementations should track channels (e.g. in a {@code ConcurrentHashMap}) and remove them
 * during per-channel close, so that {@code terminate()} only cleans up channels that were not already
 * closed individually.
 */
public interface ConnectorLifecycle {

    /**
     * Called after in-flight messages have been drained, while the transport client is still alive.
     * Use this to perform connector-specific cleanup such as flushing commits or completing handlers.
     *
     * @param channel the channel name
     * @return a {@link CompletionStage} completed when prepare-close is done
     */
    default CompletionStage<Void> prepareCloseIncoming(String channel) {
        return CompletableFuture.completedStage(null);
    }

    /**
     * Called after in-flight messages have been drained, while the transport client is still alive.
     * Use this to perform connector-specific cleanup such as flushing pending sends.
     *
     * @param channel the channel name
     * @return a {@link CompletionStage} completed when prepare-close is done
     */
    default CompletionStage<Void> prepareCloseOutgoing(String channel) {
        return CompletableFuture.completedStage(null);
    }

    /**
     * Called during the close phase to release per-channel resources.
     * Use this to close the transport client.
     *
     * @param channel the channel name
     * @return a {@link CompletionStage} completed when the channel is closed
     */
    default CompletionStage<Void> closeIncoming(String channel) {
        return CompletableFuture.completedStage(null);
    }

    /**
     * Called during the close phase to release per-channel resources.
     * Use this to close the transport client.
     *
     * @param channel the channel name
     * @return a {@link CompletionStage} completed when the channel is closed
     */
    default CompletionStage<Void> closeOutgoing(String channel) {
        return CompletableFuture.completedStage(null);
    }

    /**
     * Called once after all channels for this connector have been closed.
     * Use this to clean up connector-wide resources such as admin clients, caches, or shared connections.
     *
     * @return a {@link CompletionStage} completed when connector-wide cleanup is done
     */
    default CompletionStage<Void> terminate() {
        return CompletableFuture.completedStage(null);
    }
}
