package io.smallrye.reactive.messaging.kafka;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public class SendingKafkaMessage<K, T> implements KafkaMessage<K, T> {

    private final String topic;
    private final K key;
    private final T value;
    private final Integer partition;
    private final Long timestamp;
    // TODO Should be immutable and use a builder instead.
    private final MessageHeaders headers;
    private final Supplier<CompletionStage<Void>> ack;

    public SendingKafkaMessage(String topic, K key, T value, Long timestamp, Integer partition, MessageHeaders headers,
            Supplier<CompletionStage<Void>> ack) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.timestamp = timestamp;
        this.headers = headers;
        this.ack = ack;
    }

    @Override
    public CompletionStage<Void> ack() {
        if (ack == null) {
            return CompletableFuture.completedFuture(null);
        } else {
            return ack.get();
        }
    }

    @Override
    public T getPayload() {
        return this.value;
    }

    @Override
    public K getKey() {
        return this.key;
    }

    @Override
    public String getTopic() {
        return this.topic;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public MessageHeaders getHeaders() {
        return headers;
    }

    @Override
    public Integer getPartition() {
        return partition;
    }
}
