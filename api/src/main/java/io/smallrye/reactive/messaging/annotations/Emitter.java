package io.smallrye.reactive.messaging.annotations;

import io.smallrye.reactive.messaging.EmitterType;

/**
 * @deprecated Use {@link org.eclipse.microprofile.reactive.messaging.Emitter} instead.
 */
@Deprecated
public interface Emitter<T> extends EmitterType {

    /**
     * Sends a payload or a message to the stream.
     *
     * @param msg the <em>thing</em> to send, must not be {@code null}
     * @return the current emitter
     * @throws IllegalStateException if the stream does not have any pending requests, or if the stream has been
     *         cancelled or terminated.
     */
    Emitter<T> send(T msg);

    /**
     * Completes the stream.
     * This method sends the completion signal, no messages can be sent once this method is called.
     */
    void complete();

    /**
     * Propagates an error in the stream.
     * This methods sends an error signal, no messages can be sent once this method is called.
     *
     * @param e the exception, must not be {@code null}
     */
    void error(Exception e);

    /**
     * @return {@code true} if the emitter has been terminated or the subscription cancelled.
     */
    boolean isCancelled();

    /**
     * @return {@code true} if the subscriber accepts messages, {@code false} otherwise.
     *         Using {@link #send(Object)} on an emitter not expecting message would throw an {@link IllegalStateException}.
     */
    boolean isRequested();

}
