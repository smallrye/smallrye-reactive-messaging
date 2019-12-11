package io.smallrye.reactive.messaging.kafka;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.reactive.messaging.Headers;

public class SendingKafkaMessage<K, T> implements KafkaMessage<K, T> {

    private final T value;
    private final Supplier<CompletionStage<Void>> ack;
    private final Headers headers;

    public SendingKafkaMessage(String topic, K key, T value, long timestamp, int partition, MessageHeaders headers,
            Supplier<CompletionStage<Void>> ack) {

        Headers.HeadersBuilder builder = Headers.builder();
        if (topic != null) {
            builder.with(KafkaHeaders.TOPIC, topic);
        }
        if (key != null) {
            builder.with(KafkaHeaders.KEY, key);
        }
        if (partition >= 0) {
            builder.with(KafkaHeaders.PARTITION, partition);
        }
        if (timestamp >= 0) {
            builder.with(KafkaHeaders.TIMESTAMP, timestamp);
        }
        if (headers != null) {
            builder.with(KafkaHeaders.KAFKA_HEADERS, headers.unwrap());
        }
        this.headers = builder.build();
        this.value = value;
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

    @SuppressWarnings("unchecked")
    @Override
    public K getKey() {
        return (K) headers.get(KafkaHeaders.KEY);
    }

    @Override
    public String getTopic() {
        return headers.getAsString(KafkaHeaders.TOPIC, null);
    }

    @Override
    public long getTimestamp() {
        return headers.getAsLong(KafkaHeaders.TIMESTAMP, -1L);
    }

    @SuppressWarnings("unchecked")
    @Override
    public MessageHeaders getKafkaHeaders() {
        Iterable<Header> iterable = headers.get(KafkaHeaders.KAFKA_HEADERS, Iterable.class);
        if (iterable != null) {
            return new MessageHeaders(iterable);
        } else {
            return new MessageHeaders(Collections.emptyList());
        }
    }

    @Override
    public Supplier<CompletionStage<Void>> getAckSupplier() {
        return ack;
    }

    @Override
    public int getPartition() {
        return headers.getAsInteger(KafkaHeaders.PARTITION, -1);
    }

    @Override
    public long getOffset() {
        return headers.getAsLong(KafkaHeaders.OFFSET, -1L);
    }

    @Override
    public Headers getHeaders() {
        return headers;
    }
}
