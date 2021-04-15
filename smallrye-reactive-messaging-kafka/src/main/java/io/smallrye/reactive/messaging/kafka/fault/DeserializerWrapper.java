package io.smallrye.reactive.messaging.kafka.fault;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;

/**
 * Wraps a delegate deserializer to handle config and deserialization failures.
 *
 * @param <T> the type of object created by the deserializer.
 */
public class DeserializerWrapper<T> implements Deserializer<T> {

    /**
     * Header name for deserialization failure message.
     */
    public static final String DESERIALIZATION_FAILURE_REASON = "deserialization-failure-reason";

    /**
     * Header name for deserialization failure cause if any.
     */
    public static final String DESERIALIZATION_FAILURE_CAUSE = "deserialization-failure-cause";

    /**
     * Header name used when the deserialization failure happened on a key. The value is {@code "true"} in this case,
     * absent otherwise.
     */
    public static final String DESERIALIZATION_FAILURE_IS_KEY = "deserialization-failure-key";

    /**
     * Header name for the topic of the incoming message when a deserialization failure happen.
     */
    public static final String DESERIALIZATION_FAILURE_TOPIC = "deserialization-failure-topic";

    /**
     * Header name passing the data that was not able to be deserialized.
     */
    public static final String DESERIALIZATION_FAILURE_DATA = "deserialization-failure-data";

    /**
     * Header name passing the class name of the underlying deserializer.
     */
    public static final String DESERIALIZATION_FAILURE_DESERIALIZER = "deserialization-failure-deserializer";

    private static final byte[] TRUE_VALUE = "true".getBytes(StandardCharsets.UTF_8);

    private final Deserializer<T> delegate;

    private final boolean handleKeys;

    private final DeserializationFailureHandler<?> deserializationFailureHandler;
    private final KafkaSource<?, ?> source;

    public DeserializerWrapper(String className, boolean key, DeserializationFailureHandler<?> failureHandler,
            KafkaSource<?, ?> source) {
        this.delegate = createDelegateDeserializer(className);
        this.handleKeys = key;
        this.deserializationFailureHandler = failureHandler;
        this.source = source;
    }

    /**
     * Delegates to the underlying deserializer instance.
     *
     * @param configs the configuration
     * @param isKey the key
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        try {
            delegate.configure(configs, isKey);
        } catch (Exception e) {
            // The deserializer cannot be configured - fails and marks the application as unhealthy
            source.reportFailure(e, true);
            throw new KafkaException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private Deserializer<T> createDelegateDeserializer(String clazz) {
        try {
            return (Deserializer<T>) Utils.newInstance(clazz, Deserializer.class);
        } catch (ClassNotFoundException e) {
            throw KafkaExceptions.ex.unableToCreateInstance(clazz, e);
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return this.delegate.deserialize(topic, data);
        } catch (Exception e) {
            return tryToRecover(topic, null, data, e);
        }
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        try {
            return this.delegate.deserialize(topic, headers, data);
        } catch (Exception e) {
            return tryToRecover(topic,
                    addFailureDetailsToHeaders(topic, headers, data, e),
                    data, e);
        }
    }

    /**
     * If the user has specified a handler function - use it.
     *
     * @param topic the topic
     * @param headers the header, can be {@code null}
     * @param data the data that was not deserialized
     * @param exception the exception thrown by the underlying deserializer.
     * @return an instance of {@code <T>}, {@code null} if the user didn't specify a function (or if the function returned
     *         {@code null}).
     */
    @SuppressWarnings("unchecked")
    private T tryToRecover(String topic, Headers headers, byte[] data, Exception exception) {
        if (deserializationFailureHandler != null) {
            try {
                return (T) deserializationFailureHandler.handleDeserializationFailure(topic, this.handleKeys,
                        delegate.getClass().getName(), data, exception, headers);
            } catch (Exception e) {
                KafkaLogging.log.deserializationFailureHandlerFailure(deserializationFailureHandler.toString(), e);
                source.reportFailure(e, true);
            }
        }
        return null;
    }

    @Override
    public void close() {
        // Be a bit more defensive here as close can be called after an instantiation failure.
        if (this.delegate != null) {
            this.delegate.close();
        }
    }

    private Headers addFailureDetailsToHeaders(String topic, Headers headers, byte[] data, Exception e) {
        String message = e.getMessage();
        String cause = e.getCause() != null ? e.getCause().getMessage() : null;

        headers.add(DESERIALIZATION_FAILURE_DESERIALIZER, delegate.getClass().getName()
                .getBytes(StandardCharsets.UTF_8));
        headers.add(DESERIALIZATION_FAILURE_TOPIC, topic.getBytes(StandardCharsets.UTF_8));

        if (this.handleKeys) {
            headers.add(DESERIALIZATION_FAILURE_IS_KEY, TRUE_VALUE);
        }

        if (message != null) {
            headers.add(DESERIALIZATION_FAILURE_REASON, message.getBytes(StandardCharsets.UTF_8));
        }
        if (cause != null) {
            headers.add(DESERIALIZATION_FAILURE_CAUSE, cause.getBytes(StandardCharsets.UTF_8));
        }
        if (data != null) {
            headers.add(DESERIALIZATION_FAILURE_DATA, data);
        }

        return headers;

    }

}
