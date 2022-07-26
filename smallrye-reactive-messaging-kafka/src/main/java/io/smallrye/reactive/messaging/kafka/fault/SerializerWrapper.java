package io.smallrye.reactive.messaging.kafka.fault;

import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.SerializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging;

public class SerializerWrapper<T> implements Serializer<T> {

    private final Serializer<T> delegate;

    private final boolean handleKeys;

    private final SerializationFailureHandler<T> serializationFailureHandler;

    public SerializerWrapper(String className, boolean handleKeys, SerializationFailureHandler<T> failureHandler) {
        this.delegate = createDelegateSerializer(className);
        this.handleKeys = handleKeys;
        this.serializationFailureHandler = failureHandler;
    }

    @SuppressWarnings("unchecked")
    private Serializer<T> createDelegateSerializer(String clazz) {
        try {
            return (Serializer<T>) Utils.newInstance(clazz, Serializer.class);
        } catch (ClassNotFoundException e) {
            throw KafkaExceptions.ex.unableToCreateInstance(clazz, e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        try {
            delegate.configure(configs, isKey);
        } catch (Exception e) {
            // The serializer cannot be configured - fails and marks the application as unhealthy
            throw new KafkaException(e);
        }
    }

    @Override
    public byte[] serialize(String topic, T data) {
        return wrapSerialize(() -> this.delegate.serialize(topic, data), topic, null, data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return wrapSerialize(() -> this.delegate.serialize(topic, headers, data), topic, headers, data);
    }

    /**
     * If the user has specified a decorator function - use it.
     * Otherwise, call the serializer, in case of failure logs and throws the exception.
     *
     * @param serialize the delegated serialize function to call
     * @param topic the topic
     * @param headers the header, can be {@code null}
     * @param data the data that was not deserialized
     * @return an instance of {@code <T>}, {@code null} if the user didn't specify a function (or if the function returned
     *         {@code null}).
     */
    private byte[] wrapSerialize(Supplier<byte[]> serialize, String topic, Headers headers, T data) {
        if (serializationFailureHandler != null) {
            try {
                return serializationFailureHandler.decorateSerialization(Uni.createFrom().item(serialize),
                        topic, this.handleKeys, delegate.getClass().getName(), data, headers);
            } catch (Exception e) {
                KafkaLogging.log.serializationFailureHandlerFailure(serializationFailureHandler.toString(), e);
                throw e;
            }
        } else {
            try {
                return serialize.get();
            } catch (Exception e) {
                KafkaLogging.log.unableToSerializeMessage(topic, e);
                throw e;
            }
        }
    }

    @Override
    public void close() {
        // Be a bit more defensive here as close can be called after an instantiation failure.
        if (this.delegate != null) {
            this.delegate.close();
        }
    }
}
