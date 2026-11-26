package io.smallrye.reactive.messaging;

import io.smallrye.mutiny.Uni;

/**
 * Manages the shutdown lifecycle of messaging channels.
 * <p>
 * The shutdown is split into three phases to support external orchestrators (e.g. Quarkus service graph):
 * <ol>
 * <li><strong>Prepare close incoming</strong> ({@link #prepareCloseIncoming()}) — pauses incoming channels and waits
 * for in-flight messages to complete their full processing chain (including outgoing sends via ack chaining).
 * The application is still functional and accepting requests during this phase.</li>
 * <li><strong>Prepare close outgoing</strong> ({@link #prepareCloseOutgoing()}) — completes emitters and flushes
 * buffered messages on outgoing channels. By this point new external requests have been rejected,
 * so no new messages will arrive via emitters.</li>
 * <li><strong>Close</strong> ({@link #close()}) — closes transport clients and releases
 * connector-wide resources. This phase runs during normal container teardown.</li>
 * </ol>
 * <p>
 * In CDI environments without an external orchestrator, a {@code @BeforeDestroyed(ApplicationScoped.class)}
 * observer runs all three phases sequentially.
 * Each phase runs at most once: when an external orchestrator calls individual phases, the CDI observer skips the
 * phases that have already been started, including a phase that is still running.
 * <p>
 * <strong>Note:</strong> Drain guarantees apply to channels using {@code POST_PROCESSING} acknowledgment
 * (the default). Channels with {@code PRE_PROCESSING} acknowledge messages before processing completes,
 * so drain cannot guarantee that in-flight processing has finished.
 */
public interface ChannelLifecycleManager {

    /**
     * Prepares all incoming channels for close: pauses each channel and waits for in-flight messages
     * to be acknowledged, then calls connector prepare-close hooks.
     * <p>
     * This phase requires outgoing channels to remain operational — in-flight messages
     * may still be flowing through the processing chain to outgoing channels via ack chaining.
     * <p>
     * This method runs at most once: later calls return immediately, even while the first call is still running.
     * Blocking, with a timeout derived from the longest channel drain timeout, configurable with the
     * {@code smallrye.messaging.graceful-shutdown.phase-timeout} property (milliseconds).
     * It waits for {@link #prepareCloseIncomingAsync()}, and must not be called from a thread that cannot block.
     */
    default void prepareCloseIncoming() {
        prepareCloseIncomingAsync().await().indefinitely();
    }

    /**
     * The non-blocking variant of {@link #prepareCloseIncoming()}, for shutdown orchestrators with asynchronous
     * handlers.
     * <p>
     * The phase starts when the returned {@link Uni} is subscribed, and runs at most once, whichever variant starts
     * it: a later subscription completes immediately, even while the phase is still running. The {@link Uni} completes
     * when the phase ends, at most after the phase timeout; failures, including the timeout, are logged and do not
     * fail it.
     *
     * @return a {@link Uni} completed when the incoming channels are prepared for close
     */
    Uni<Void> prepareCloseIncomingAsync();

    /**
     * Prepares all outgoing channels for close: completes emitters, flushes buffered messages,
     * and calls connector prepare-close hooks.
     * <p>
     * This should run after new external requests have been rejected, so no new messages
     * arrive via emitters while draining.
     * <p>
     * This method runs at most once: later calls return immediately, even while the first call is still running.
     * Blocking, with a timeout derived from the longest channel drain timeout, configurable with the
     * {@code smallrye.messaging.graceful-shutdown.phase-timeout} property (milliseconds).
     * It waits for {@link #prepareCloseOutgoingAsync()}, and must not be called from a thread that cannot block.
     */
    default void prepareCloseOutgoing() {
        prepareCloseOutgoingAsync().await().indefinitely();
    }

    /**
     * The non-blocking variant of {@link #prepareCloseOutgoing()}, with the same guarantees as
     * {@link #prepareCloseIncomingAsync()}.
     *
     * @return a {@link Uni} completed when the outgoing channels are prepared for close
     */
    Uni<Void> prepareCloseOutgoingAsync();

    /**
     * Closes transport clients per channel and terminates connector-wide resources
     * (e.g. shared admin clients, connection pools).
     * <p>
     * This should run after both {@link #prepareCloseIncoming()} and {@link #prepareCloseOutgoing()}
     * have completed, typically during ordered container teardown.
     * <p>
     * This method runs at most once: later calls return immediately, even while the first call is still running.
     * Blocking, with a timeout derived from the longest channel drain timeout, configurable with the
     * {@code smallrye.messaging.graceful-shutdown.phase-timeout} property (milliseconds), for each of its steps:
     * closing the incoming channels, closing the outgoing channels, and terminating the connectors.
     * It waits for {@link #closeAsync()}, and must not be called from a thread that cannot block.
     */
    default void close() {
        closeAsync().await().indefinitely();
    }

    /**
     * The non-blocking variant of {@link #close()}, with the same guarantees as {@link #prepareCloseIncomingAsync()}.
     *
     * @return a {@link Uni} completed when the channels are closed and the connectors terminated
     */
    Uni<Void> closeAsync();

    /**
     * Prepares a single incoming channel for close: pauses, waits for in-flight messages
     * to be acknowledged, then calls the connector's prepare-close hook.
     * <p>
     * The channel is paused when this method is called, so that it stops receiving messages right away; the wait and
     * the connector's hook run when the returned {@link Uni} is subscribed.
     *
     * @param channel the channel name
     * @return a {@link Uni} completed when the channel is prepared for close
     */
    Uni<Void> prepareCloseIncoming(String channel);

    /**
     * Prepares a single outgoing channel for close: completes the emitter, flushes buffered messages,
     * then calls the connector's prepare-close hook. Nothing happens until the returned {@link Uni} is subscribed.
     *
     * @param channel the channel name
     * @return a {@link Uni} completed when the channel is prepared for close
     */
    Uni<Void> prepareCloseOutgoing(String channel);

    /**
     * Closes a single incoming channel's transport client.
     *
     * @param channel the channel name
     * @return a {@link Uni} completed when the channel is closed
     */
    Uni<Void> closeIncoming(String channel);

    /**
     * Closes a single outgoing channel's transport client.
     *
     * @param channel the channel name
     * @return a {@link Uni} completed when the channel is closed
     */
    Uni<Void> closeOutgoing(String channel);

}
