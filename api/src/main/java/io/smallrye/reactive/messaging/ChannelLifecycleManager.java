package io.smallrye.reactive.messaging;

import io.smallrye.mutiny.Uni;

/**
 * Manages the full lifecycle of messaging channels: creation, query, and shutdown.
 * <p>
 * The shutdown sequence for each channel is:
 * <ol>
 * <li>Drain in-flight messages via the pausable channel (if available)</li>
 * <li>Connector pre-shutdown (e.g. flush commits)</li>
 * <li>Connector shutdown (e.g. close transport client)</li>
 * </ol>
 */
public interface ChannelLifecycleManager {

    /**
     * Shuts down an incoming channel through the full lifecycle sequence:
     * drain, pre-shutdown, shutdown.
     *
     * @param channel the channel name
     * @return a {@link Uni} completed when the channel is fully shut down
     */
    Uni<Void> shutdownIncoming(String channel);

    /**
     * Shuts down an outgoing channel through the full lifecycle sequence:
     * drain, pre-shutdown, shutdown.
     *
     * @param channel the channel name
     * @return a {@link Uni} completed when the channel is fully shut down
     */
    Uni<Void> shutdownOutgoing(String channel);

}
