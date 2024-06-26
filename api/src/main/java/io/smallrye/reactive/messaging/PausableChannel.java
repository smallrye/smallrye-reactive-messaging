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
}
