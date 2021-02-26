package io.smallrye.reactive.messaging.kafka;

import org.apache.kafka.common.header.Headers;

/**
 * Bean invoked on Kafka deserialization failure.
 * <p>
 * Implementors must use {@code @Named} to provide a name to the bean.
 * This name is then referenced in the channel configuration:
 * {@code mp.messaging.incoming.my-channel.[key|value]-deserialization-failure-handler=name}.
 * <p>
 * When a Kafka Record's key or value cannot be deserialized, this bean is called to provide a fallback value.
 * {@code null} is an accepted fallback value.
 * If this bean throws an exception, this is considered as a fatal failure and the application is reported unhealthy.
 *
 * @param <T> the expected type
 */
public interface DeserializationFailureHandler<T> {

    /**
     * Handles a deserialization issue for a record's key.
     *
     * @param topic the topic
     * @param isKey whether the failure happened when deserializing a record's key.
     * @param deserializer the used deserializer
     * @param data the data that was not deserialized correctly
     * @param exception the exception
     * @param headers the record headers, extended with the failure reason, causes, and data. May also be {@code null}
     * @return the fallback {@code T}
     */
    default T handleDeserializationFailure(String topic, boolean isKey, String deserializer, byte[] data,
            Exception exception, Headers headers) {
        return null;
    }

}
