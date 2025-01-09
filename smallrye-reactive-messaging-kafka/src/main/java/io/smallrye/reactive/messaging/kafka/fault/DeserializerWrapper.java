package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.addFailureDetailsToHeaders;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging;
import io.smallrye.reactive.messaging.kafka.impl.ce.KafkaCloudEventHelper;

/**
 * Wraps a delegate deserializer to handle config and deserialization failures.
 *
 * @param <T> the type of object created by the deserializer.
 */
public class DeserializerWrapper<T> implements Deserializer<T> {

    private final Deserializer<T> delegate;

    private final boolean handleKeys;

    private final DeserializationFailureHandler<T> deserializationFailureHandler;
    private final BiConsumer<Throwable, Boolean> reportFailure;

    private final boolean failOnDeserializationErrorWithoutHandler;
    private final boolean cloudEventsEnabled;

    public DeserializerWrapper(String className, boolean key, DeserializationFailureHandler<T> failureHandler,
            BiConsumer<Throwable, Boolean> reportFailure, boolean failByDefault, boolean cloudEventsEnabled) {
        this.delegate = createDelegateDeserializer(className);
        this.handleKeys = key;
        this.deserializationFailureHandler = failureHandler;
        this.reportFailure = reportFailure;
        this.failOnDeserializationErrorWithoutHandler = failByDefault;
        this.cloudEventsEnabled = cloudEventsEnabled;
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
            reportFailure.accept(e, true);
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
        return wrapDeserialize(() -> handleCloudEvents(this.delegate.deserialize(topic, data), null), topic, null, data);
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        return wrapDeserialize(() -> handleCloudEvents(this.delegate.deserialize(topic, headers, data), headers), topic,
                headers, data);
    }

    /**
     * If the user has specified a decorator function - use it.
     * Otherwise, call the deserializer, in case of failure the outcome depends on the {@code fail-on-deserialization-failure}
     * attribute.
     * If {@code fail-on-deserialization-failure} is set to {@code true} (default), this method throws a {@link KafkaException}.
     * If set to {@code false}, it recovers with {@code null}.
     *
     * @param deserialize the delegated deserialize function to call
     * @param topic the topic
     * @param headers the header, can be {@code null}
     * @param data the data that was not deserialized
     * @return an instance of {@code <T>}, {@code null} if the user didn't specify a function (or if the function returned
     *         {@code null}).
     */
    private T wrapDeserialize(Supplier<T> deserialize, String topic, Headers headers, byte[] data) {
        if (deserializationFailureHandler != null) {
            try {
                return deserializationFailureHandler.decorateDeserialization(Uni.createFrom().item(deserialize),
                        topic, this.handleKeys, delegate.getClass().getName(), data, headers);
            } catch (Exception e) {
                KafkaLogging.log.deserializationFailureHandlerFailure(deserializationFailureHandler.toString(), e);
                reportFailure.accept(e, true);
                if (e instanceof KafkaException) {
                    throw (KafkaException) e;
                }
                throw new KafkaException(e);
            }
        } else {
            try {
                return deserialize.get();
            } catch (Exception e) {
                if (failOnDeserializationErrorWithoutHandler) {
                    KafkaLogging.log.unableToDeserializeMessage(topic, e);
                    reportFailure.accept(e, true);
                    if (e instanceof KafkaException) {
                        throw (KafkaException) e;
                    }
                    throw new KafkaException(e);
                }
                // insert failure details to headers
                addFailureDetailsToHeaders(delegate.getClass().getName(), topic, handleKeys, headers, data, e);
                // fallback to null
                return null;
            }
        }
    }

    private T handleCloudEvents(T payload, Headers headers) {
        if (cloudEventsEnabled && !handleKeys && headers != null) {
            KafkaCloudEventHelper.CloudEventMode mode = KafkaCloudEventHelper.getCloudEventMode(headers);
            switch (mode) {
                case STRUCTURED:
                    //noinspection unchecked
                    return (T) KafkaCloudEventHelper.parseStructuredContent(payload);
                case BINARY:
                    return KafkaCloudEventHelper.checkBinaryRecord(payload, headers);
                case NOT_A_CLOUD_EVENT:
                    return payload;
            }
        }
        return payload;
    }

    @Override
    public void close() {
        // Be a bit more defensive here as close can be called after an instantiation failure.
        if (this.delegate != null) {
            this.delegate.close();
        }
    }

}
