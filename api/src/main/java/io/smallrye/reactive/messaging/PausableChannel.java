package io.smallrye.reactive.messaging;

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
}
