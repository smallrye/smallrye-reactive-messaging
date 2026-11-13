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
 * When an external orchestrator calls individual phases, the CDI observer skips already-completed phases.
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
     * This method is idempotent. Blocking, with a global timeout of 30 seconds.
     */
    void prepareCloseIncoming();

    /**
     * Prepares all outgoing channels for close: completes emitters, flushes buffered messages,
     * and calls connector prepare-close hooks.
     * <p>
     * This should run after new external requests have been rejected, so no new messages
     * arrive via emitters while draining.
     * <p>
     * This method is idempotent. Blocking, with a global timeout of 30 seconds.
     */
    void prepareCloseOutgoing();

    /**
     * Closes transport clients per channel and terminates connector-wide resources
     * (e.g. shared admin clients, connection pools).
     * <p>
     * This should run after both {@link #prepareCloseIncoming()} and {@link #prepareCloseOutgoing()}
     * have completed, typically during ordered container teardown.
     * <p>
     * This method is idempotent. Blocking, with a global timeout of 30 seconds.
     */
    void close();

    /**
     * Prepares a single incoming channel for close: pauses, waits for in-flight messages
     * to be acknowledged, then calls the connector's prepare-close hook.
     *
     * @param channel the channel name
     * @return a {@link Uni} completed when the channel is prepared for close
     */
    Uni<Void> prepareCloseIncoming(String channel);

    /**
     * Prepares a single outgoing channel for close: completes the emitter, flushes buffered messages,
     * then calls the connector's prepare-close hook.
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
