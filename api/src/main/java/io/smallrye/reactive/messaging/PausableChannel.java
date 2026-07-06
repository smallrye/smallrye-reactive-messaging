package io.smallrye.reactive.messaging;

import java.time.Duration;

import io.smallrye.mutiny.Uni;

/**
 * A channel that can be paused and resumed.
 */
public interface PausableChannel {

    /**
     * Checks whether the channel is paused.
     *
     * @return {@code true} if the channel is paused, {@code false} otherwise
     */
    boolean isPaused();

    /**
     * Pauses the channel.
     */
    void pause();

    /**
     * Resumes the channel.
     */
    void resume();

    /**
     * Clears the buffer of the channel, discarding any items that have been
     * requested from upstream but not yet delivered to the consumer.
     */
    default void clearBuffer() {
    }

    /**
     * Pauses the channel if not already paused, then waits for all in-flight messages
     * (delivered to the consumer but not yet acked or nacked) to complete.
     * <p>
     * This is useful before performing operations that require no in-flight processing,
     * such as seeking a Kafka consumer to a new position or during graceful shutdown.
     *
     * @return a {@link Uni} that completes when all in-flight messages
     *         have been acknowledged or negatively acknowledged
     */
    default Uni<Void> pauseAndDrain() {
        return Uni.createFrom().voidItem();
    }

    /**
     * Returns the maximum duration to wait for in-flight messages to drain
     * during graceful shutdown.
     * <p>
     * Configurable via the {@code graceful-shutdown.drain-timeout} channel property.
     * Defaults to 10 seconds.
     *
     * @return the drain timeout duration
     */
    default Duration getDrainTimeout() {
        return Duration.ofSeconds(10);
    }
}
