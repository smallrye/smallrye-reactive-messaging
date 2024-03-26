package io.smallrye.reactive.messaging;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

/**
 * A generic payload that can be used to wrap a payload with metadata.
 * Allows associating a payload with metadata to be sent as a message,
 * without using signatures supporting {@code Message<T>}.
 *
 * @param <T> the type of the payload
 */
public class GenericPayload<T> {

    /**
     * Creates a new payload with the given payload and empty metadata.
     *
     * @param payload the payload
     * @param <T> the type of the payload
     * @return the payload
     */
    public static <T> GenericPayload<T> of(T payload) {
        return new GenericPayload<>(payload, Metadata.empty());
    }

    /**
     * Creates a new payload with the given payload and metadata.
     *
     * @param payload the payload
     * @param metadata the metadata
     * @param <T> the type of the payload
     * @return the payload
     */
    public static <T> GenericPayload<T> of(T payload, Metadata metadata) {
        return new GenericPayload<>(payload, metadata);
    }

    /**
     * Creates a new payload from the given message.
     *
     * @param message the message
     * @param <T> the type of the payload
     * @return the payload
     */
    public static <T> GenericPayload<T> from(Message<T> message) {
        return new GenericPayload<>(message.getPayload(), message.getMetadata());
    }

    private final T payload;
    private final Metadata metadata;

    public GenericPayload(T payload, Metadata metadata) {
        this.payload = payload;
        this.metadata = metadata;
    }

    /**
     * Gets the payload associated with this payload.
     *
     * @return the payload
     */
    public T getPayload() {
        return payload;
    }

    /**
     * Gets the metadata associated with this payload.
     *
     * @return the metadata
     */
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * Adds metadata to this payload.
     *
     * @param metadata the metadata to add
     * @return a new payload with the added metadata
     */
    public GenericPayload<T> withMetadata(Metadata metadata) {
        return GenericPayload.of(this.payload, metadata);
    }

    /**
     * Adds metadata to this payload.
     *
     * @param payload the payload to add
     * @return a new payload with the added metadata
     */
    public <R> GenericPayload<R> withPayload(R payload) {
        return GenericPayload.of(payload, this.metadata);
    }

    /**
     * Converts this payload to a message.
     *
     * @return the message with the payload and metadata
     */
    public Message<T> toMessage() {
        return Message.of(payload, metadata);
    }

    /**
     * Converts this payload to a message, merging the metadata with the given message.
     *
     * @param message the message to merge the metadata with
     * @return the message with the payload and merged metadata
     */
    public Message<T> toMessage(Message<?> message) {
        Metadata merged = Messages.merge(message.getMetadata(), this.metadata);
        return message.withPayload(payload).withMetadata(merged);
    }

}
