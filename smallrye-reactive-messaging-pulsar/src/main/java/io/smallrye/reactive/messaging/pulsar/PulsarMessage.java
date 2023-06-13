package io.smallrye.reactive.messaging.pulsar;

import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public interface PulsarMessage<T> extends ContextAwareMessage<T> {

    /**
     * Creates a new outgoing Pulsar message.
     *
     * @param message the incoming message
     * @return the new outgoing Pulsar message
     * @param <T> the type of the value
     */
    static <T> PulsarOutgoingMessage<T> from(Message<T> message) {
        return PulsarOutgoingMessage.from(message);
    }

    /**
     * Creates a new outgoing Pulsar message.
     *
     * @param payload the payload of the message
     * @return the new outgoing Pulsar message
     * @param <T> the type of the value
     */
    static <T> PulsarMessage<T> of(T payload) {
        return new PulsarOutgoingMessage<>(payload, null, null);
    }

    /**
     * Creates a new outgoing Pulsar message.
     *
     * @param payload the payload of the message
     * @param key the key of the message
     * @return the new outgoing Pulsar message
     * @param <T> the type of the value
     */
    static <T> PulsarMessage<T> of(T payload, String key) {
        return new PulsarOutgoingMessage<>(payload, null, null, PulsarOutgoingMessageMetadata.builder()
                .withKey(key)
                .build());
    }

    /**
     * Creates a new outgoing Pulsar message.
     *
     * @param payload the payload of the message
     * @param keyBytes the key of the message in bytes
     * @return the new outgoing Pulsar message
     * @param <T> the type of the value
     */
    static <T> PulsarMessage<T> of(T payload, byte[] keyBytes) {
        return new PulsarOutgoingMessage<>(payload, null, null, PulsarOutgoingMessageMetadata.builder()
                .withKeyBytes(keyBytes)
                .build());
    }

    /**
     * Creates a new outgoing Pulsar message.
     *
     * @param payload the payload of the message
     * @param metadata the outgoing message metadata
     * @return the new outgoing Pulsar message
     * @param <T> the type of the value
     */
    static <T> PulsarMessage<T> of(T payload, PulsarOutgoingMessageMetadata metadata) {
        return new PulsarOutgoingMessage<>(payload, null, null, metadata);
    }

    String getKey();

    byte[] getKeyBytes();

    boolean hasKey();

    byte[] getOrderingKey();

    Map<String, String> getProperties();

    long getEventTime();

    long getSequenceId();

}
