package io.smallrye.reactive.messaging.memory;

/**
 * Allows interacting with an in-memory source.
 * An in-memory source is a channel in which you can inject messages using this API.
 *
 * @param <T> the type of payload or message.
 */
public interface InMemorySource<T> {

    /**
     * @return the channel name.
     */
    String name();

    /**
     * Sends a message or a payload to the channel.
     *
     * @param messageOrPayload the message or payload to send. In the case of a payload, a simple message is created.
     *        Must not be {@code null}
     * @return this to allow chaining calls.
     */
    InMemorySource<T> send(T messageOrPayload);

    /**
     * Sends the completion event.
     */
    void complete();

    /**
     * Sends a failure.
     *
     * @param failure the failure, must not be {@code null}
     */
    void fail(Throwable failure);
}
