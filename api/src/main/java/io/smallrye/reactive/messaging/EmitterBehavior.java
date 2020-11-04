package io.smallrye.reactive.messaging;

public interface EmitterBehavior {
    /**
     * Sends the completion event to the channel indicating that no other events will be sent afterward.
     */
    void complete();

    /**
     * Sends a failure event to the channel. No more events will be sent afterward.
     *
     * @param e the exception, must not be {@code null}
     */
    void error(Exception e);

    /**
     * @return {@code true} if the emitter has been terminated or the subscription cancelled.
     */
    boolean isCancelled();

    /**
     * @return {@code true} if one or more subscribers request messages from the corresponding channel where the emitter
     *         connects to,
     *         return {@code false} otherwise.
     */
    boolean hasRequests();
}
