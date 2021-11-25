package io.smallrye.reactive.messaging.kafka;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Headers;

import io.smallrye.mutiny.Uni;

/**
 * Bean invoked on Kafka serialization failure.
 * <p>
 * Implementors must use {@code @Identifier} to provide a name to the bean.
 * This name is then referenced in the channel configuration:
 * {@code mp.messaging.outgoing.my-channel.[key|value]-serialization-failure-handler=name}.
 * <p>
 * When a Kafka Record's key or value cannot be serialized, this bean is called to provide failure handling,
 * such as retry or fallback value, {@code null} is an accepted fallback value.
 * If this bean throws an exception, this is considered as a fatal failure and the application is reported unhealthy.
 *
 * @param <T> the expected type
 */
public interface SerializationFailureHandler<T> {

    /**
     * Handles a serialization issue for a record's key or value.
     *
     * @param topic the topic
     * @param isKey whether the failure happened when deserializing a record's key.
     * @param serializer the used deserializer
     * @param data the data that was not deserialized correctly
     * @param exception the exception
     * @param headers the record headers, extended with the failure reason, causes, and data. May also be {@code null}
     * @return the fallback {@code T}
     */
    default byte[] handleSerializationFailure(String topic, boolean isKey, String serializer, T data,
            Exception exception, Headers headers) {
        return null;
    }

    /**
     * Decorate the given wrapped serialization action to apply fault tolerance actions.
     * The default implementation calls {@link #handleSerializationFailure} for retro compatibility.
     *
     * @param serialization the serialization call wrapped in {@link Uni}
     * @param topic the topic
     * @param isKey whether the deserialization is for a record's key.
     * @param serializer the used serializer
     * @param data the data to serialize
     * @param headers the record headers. May be {@code null}
     * @return the recovered serialization result byte array
     */
    default byte[] decorateSerialization(Uni<byte[]> serialization, String topic, boolean isKey, String serializer, T data,
            Headers headers) {
        return serialization.onFailure().recoverWithItem(
                throwable -> handleSerializationFailure(topic, isKey, serializer, data,
                        throwable instanceof Exception ? (Exception) throwable : new KafkaException(throwable), headers))
                .await().indefinitely();
    }
}
