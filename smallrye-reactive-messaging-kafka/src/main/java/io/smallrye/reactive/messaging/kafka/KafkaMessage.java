package io.smallrye.reactive.messaging.kafka;

import java.nio.charset.Charset;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;

public interface KafkaMessage<K, T> extends Message<T> {

    /**
     * Creates a new outgoing kafka message.
     *
     * @param key the key, can be {@code null}
     * @param value the value / payload, must not be {@code null}
     * @param <K> the type of the key
     * @param <T> the type of the value
     * @return the new outgoing kafka message
     */
    static <K, T> KafkaMessage<K, T> of(K key, T value) {
        return new SendingKafkaMessage<>(null, key, value, null, null, new MessageHeaders(), null);
    }

    /**
     * Creates a new Kafka Message with a header added to the header list.
     * This method produces a outgoing message.
     *
     * @param key the header key
     * @param content the header value, must not be {@code null}
     * @return the updated Kafka Message.
     */
    default KafkaMessage<K, T> withHeader(String key, byte[] content) {
        MessageHeaders headers = getHeaders().clone();
        headers.put(key, content);
        return new SendingKafkaMessage<>(getTopic(), getKey(), getPayload(), getTimestamp(), getPartition(), headers,
                getAckSupplier());
    }

    /**
     * Creates a new Kafka Message with a header added to the header list.
     * This method produces a outgoing message.
     *
     * @param key the header key
     * @param content the header value, must not be {@code null}
     * @param enc the encoding, must not be {@code null}
     * @return the updated Kafka Message.
     */
    default KafkaMessage<K, T> withHeader(String key, String content, Charset enc) {
        MessageHeaders headers = getHeaders();
        headers.put(key, content, enc);
        return new SendingKafkaMessage<>(getTopic(), getKey(), getPayload(), getTimestamp(), getPartition(), headers,
                getAckSupplier());
    }

    /**
     * Creates a new outgoing kafka message.
     *
     * @param topic the topic, must not be {@code null}
     * @param key the key, can be {@code null}
     * @param value the value / payload, must not be {@code null}
     * @param <K> the type of the key
     * @param <T> the type of the value
     * @return the new outgoing kafka message
     */
    static <K, T> KafkaMessage<K, T> of(String topic, K key, T value) {
        return new SendingKafkaMessage<>(topic, key, value, null, null, new MessageHeaders(), null);
    }

    /**
     * Creates a new outgoing kafka message.
     *
     * @param topic the topic, must not be {@code null}
     * @param key the key, can be {@code null}
     * @param value the value / payload, must not be {@code null}
     * @param timestamp the timestamp, can be {@code null}
     * @param partition the partition, can be {@code null}
     * @param <K> the type of the key
     * @param <T> the type of the value
     * @return the new outgoing kafka message
     */
    static <K, T> KafkaMessage<K, T> of(String topic, K key, T value, Long timestamp, Integer partition) {
        return new SendingKafkaMessage<>(topic, key, value, timestamp, partition, new MessageHeaders(), null);
    }

    /**
     * Creates a new Kafka Message with a specific acknowledgement.
     * This method produces a outgoing message.
     *
     * @param ack the acknowledgement action supplier, must not be {@code null}
     * @return the new Kafka message
     */
    default KafkaMessage<K, T> withAck(Supplier<CompletionStage<Void>> ack) {
        return new SendingKafkaMessage<>(getTopic(), getKey(), getPayload(), getTimestamp(), getPartition(), getHeaders(),
                ack);
    }

    /**
     * Gets the payload of the message. It returns the value of the Kafka record.
     *
     * @return the payload
     */
    T getPayload();

    /**
     * Gets the key of the record, can be {@code null}.
     *
     * @return the key, can be {@code null}
     */
    K getKey();

    /**
     * Gets the Kafka topic on which the record has been sent or received.
     *
     * @return the topic, for outgoing message, it can be {@code null} and use default topic configured in the connector.
     */
    String getTopic();

    /**
     * Gets the partition on which the record is sent or received.
     *
     * @return the partition, {@code null} or {@code negative} indicates that the partition is not set
     */
    Integer getPartition();

    /**
     * @return the record timestamp, {@code null} if not set.
     */
    Long getTimestamp();

    /**
     * @return the message headers.
     */
    MessageHeaders getHeaders();

    /**
     * @return the supplier producing the acknowledgement action.
     */
    Supplier<CompletionStage<Void>> getAckSupplier();

}
