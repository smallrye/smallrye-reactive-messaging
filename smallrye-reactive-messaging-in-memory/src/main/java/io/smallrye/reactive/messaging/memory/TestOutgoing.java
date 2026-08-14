package io.smallrye.reactive.messaging.memory;

import java.util.List;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Allows interacting with a test outgoing channel.
 * Use this API to verify messages sent by the application during tests.
 *
 * @param <T> the type payload expected in the received messages.
 */
public interface TestOutgoing<T> {

    /**
     * @return the channel name.
     */
    String name();

    /**
     * @return the list, potentially empty, of messages sent by the application to this channel.
     *         The implementation must return a copy of the list.
     *         The {@link #clear()} method allows flushing the list.
     */
    List<? extends Message<T>> sent();

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
