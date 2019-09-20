package io.smallrye.reactive.messaging.kafka;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import io.opentracing.Span;
import org.eclipse.microprofile.reactive.messaging.Message;

public interface KafkaMessage<K, T> extends Message<T> {

    static <K, T> KafkaMessage<K, T> of(K key, T value) {
        return new SendingKafkaMessage<>(null, key, value, null, null, new MessageHeaders(), null, null);
    }

    static <K, T> KafkaMessage<K, T> ofAndTraced(K key, T value, Span span) {
        return new SendingKafkaMessage<>(null, key, value, null, null, new MessageHeaders(), null, span);
    }

    static <K, T> KafkaMessage<K, T> of(String topic, K key, T value) {
        return new SendingKafkaMessage<>(topic, key, value, null, null, new MessageHeaders(), null, null);
    }

    static <K, T> KafkaMessage<K, T> of(String topic, K key, T value, Long timestamp, Integer partition) {
        return new SendingKafkaMessage<>(topic, key, value, timestamp, partition, new MessageHeaders(), null, null);
    }

    default KafkaMessage<K, T> withAck(Supplier<CompletionStage<Void>> ack) {
        return new SendingKafkaMessage<>(getTopic(), getKey(), getPayload(), getTimestamp(), getPartition(), getHeaders(), ack,
                null);
    }

    Span getSpan();

    T getPayload();

    K getKey();

    String getTopic();

    Integer getPartition();

    Long getTimestamp();

    MessageHeaders getHeaders();

}
