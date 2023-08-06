package io.smallrye.reactive.messaging.kafka;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Headers;

import io.smallrye.mutiny.Uni;

/**
 * Bean invoked on Kafka deserialization to implement custom failure handling.
 * <p>
 * Implementors must use {@code @Identifier} to provide a name to the bean.
 * This name is then referenced in the channel configuration:
 * {@code mp.messaging.incoming.my-channel.[key|value]-deserialization-failure-handler=name}.
 * <p>
 * In case a Kafka Record's key or value cannot be deserialized, this bean can provide failure handling via Mutiny's
 * {@link Uni} interface to retry or to provide a fallback value.
 * {@code null} is an accepted fallback value.
 * If this bean throws an exception, this is considered as a fatal failure and the application is reported unhealthy.
 *
 * @param <T> the expected type
 */
public interface DeserializationFailureHandler<T> {

    /**
     * Header name for deserialization failure message.
     */
    String DESERIALIZATION_FAILURE_REASON = "deserialization-failure-reason";

    /**
     * Header name for deserialization failure cause if any.
     */
    String DESERIALIZATION_FAILURE_CAUSE = "deserialization-failure-cause";

    /**
     * Header name used when the deserialization failure happened on a key. The value is {@code "true"} in this case,
     * absent otherwise.
     */
    String DESERIALIZATION_FAILURE_IS_KEY = "deserialization-failure-key";

    /**
     * Header name for the topic of the incoming message when a deserialization failure happen.
     */
    String DESERIALIZATION_FAILURE_TOPIC = "deserialization-failure-topic";

    /**
     * Header name passing the data that was not able to be deserialized.
     */
    String DESERIALIZATION_FAILURE_DATA = "deserialization-failure-data";

    /**
     * Header name passing the key data that was not able to be deserialized.
     */
    String DESERIALIZATION_FAILURE_KEY_DATA = "deserialization-failure-key-data";

    /**
     * Header name passing the value data that was not able to be deserialized.
     */
    String DESERIALIZATION_FAILURE_VALUE_DATA = "deserialization-failure-value-data";

    /**
     * Header name passing the class name of the underlying deserializer.
     */
    String DESERIALIZATION_FAILURE_DESERIALIZER = "deserialization-failure-deserializer";

    /**
     * Header name passing the class name of the underlying deserializer.
     */
    String DESERIALIZATION_FAILURE_DLQ = "deserialization-failure-dlq";

    byte[] TRUE_VALUE = "true".getBytes(StandardCharsets.UTF_8);

    /**
     * Handles a deserialization issue for a record's key or value.
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

    /**
     * Decorate the given wrapped deserialization action to apply fault tolerance actions.
     * The default implementation calls {@link #handleDeserializationFailure} for retro compatibility.
     *
     * @param deserialization the deserialization call wrapped in {@link Uni}
     * @param topic the topic
     * @param isKey whether the deserialization is for a record's key.
     * @param deserializer the used deserializer
     * @param data the data to deserialize
     * @param headers the record headers. May be {@code null}
     * @return the recovered {@code T}
     */
    default T decorateDeserialization(Uni<T> deserialization, String topic, boolean isKey, String deserializer, byte[] data,
            Headers headers) {
        return deserialization.onFailure().recoverWithItem(
                throwable -> handleDeserializationFailure(topic, isKey, deserializer, data,
                        throwable instanceof Exception ? (Exception) throwable : new KafkaException(throwable),
                        addFailureDetailsToHeaders(deserializer, topic, isKey, headers, data, throwable)))
                .await().indefinitely();
    }

    /**
     * Extend record headers to add deserialization failure reason to the incoming message headers
     *
     * @param deserializer deserializer class name
     * @param topic the topic
     * @param isKey whether the failure happened when deserializing a record's key.
     * @param headers the record headers
     * @param data the data that was not deserialized correctly
     * @param failure the deserilization failure
     * @return headers
     */
    static Headers addFailureDetailsToHeaders(String deserializer, String topic, boolean isKey, Headers headers, byte[] data,
            Throwable failure) {
        String message = failure.getMessage();
        String cause = failure.getCause() != null ? failure.getCause().getMessage() : null;

        if (headers != null) {
            headers.add(DESERIALIZATION_FAILURE_DESERIALIZER, deserializer.getBytes(StandardCharsets.UTF_8));
            headers.add(DESERIALIZATION_FAILURE_DLQ, TRUE_VALUE);
            headers.add(DESERIALIZATION_FAILURE_TOPIC, topic.getBytes(StandardCharsets.UTF_8));

            if (isKey) {
                headers.add(DESERIALIZATION_FAILURE_IS_KEY, TRUE_VALUE);
            }

            if (message != null) {
                headers.add(DESERIALIZATION_FAILURE_REASON, message.getBytes(StandardCharsets.UTF_8));
            }
            if (cause != null) {
                headers.add(DESERIALIZATION_FAILURE_CAUSE, cause.getBytes(StandardCharsets.UTF_8));
            }
            if (data != null) {
                if (isKey) {
                    headers.add(DESERIALIZATION_FAILURE_KEY_DATA, data);
                } else {
                    headers.add(DESERIALIZATION_FAILURE_VALUE_DATA, data);
                }
                // Do not break retro-compatibility
                headers.add(DESERIALIZATION_FAILURE_DATA, data);
            }
        }

        return headers;
    }

}
