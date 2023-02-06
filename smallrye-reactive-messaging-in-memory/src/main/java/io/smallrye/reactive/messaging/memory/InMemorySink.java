package io.smallrye.reactive.messaging.memory;

import java.util.List;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Allows interacting with an in-memory sink.
 * An in-memory sink is a channel in which you can observes the received messages and events.
 *
 * @param <T> the type payload expected in the received messages.
 */
public interface InMemorySink<T> {

    /**
     * @return the channel name.
     */
    String name();

    /**
     * @return the list, potentially empty, of the received messages. The implementation must return a copy of the list.
     *         The {@link #clear()} method allows flushing the list.
     */
    List<? extends Message<T>> received();

    /**
     * Clears the list of received messages. It also reset the received failure (if any) and the received completion
     * event.
     */
    void clear();

    /**
     * @return {@code true} if the channel received the completion event.
     */
    boolean hasCompleted();

    /**
     * @return {@code true} if the channel received the failure event.
     */
    boolean hasFailed();

    /**
     * @return the failure if {@link #hasFailed()} returned {@code true}.
     */
    Throwable getFailure();

}
