package io.smallrye.reactive.messaging.kafka;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public class SendingKafkaMessage<K, T> implements KafkaMessage<K, T> {

    private final String topic;
    private final K key;
    private final T value;
    private final int partition;
    private final long timestamp;
    private final MessageHeaders headers;
    private final Supplier<CompletionStage<Void>> ack;

    public SendingKafkaMessage(String topic, K key, T value, long timestamp, int partition, MessageHeaders headers,
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
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public MessageHeaders getMessageHeaders() {
        return headers;
    }

    @Override
    public Supplier<CompletionStage<Void>> getAckSupplier() {
        return ack;
    }

    @Override
    public int getPartition() {
        if (partition < 0) {
            return -1;
        }
        return partition;
    }

    @Override
    public long getOffset() {
        return -1;
    }
}
