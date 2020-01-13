package io.smallrye.reactive.messaging.kafka;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.reactive.messaging.Metadata;

public class SendingKafkaMessage<K, T> implements KafkaMessage<K, T> {

    private final T value;
    private final Supplier<CompletionStage<Void>> ack;
    private final Metadata metadata;

    public SendingKafkaMessage(String topic, K key, T value, long timestamp, int partition, MessageHeaders headers,
            Supplier<CompletionStage<Void>> ack) {

        Metadata.MetadataBuilder builder = Metadata.builder();
        if (topic != null) {
            builder.with(KafkaMetadata.OUTGOING_TOPIC, topic);
        }
        if (key != null) {
            builder.with(KafkaMetadata.OUTGOING_KEY, key);
        }
        if (partition >= 0) {
            builder.with(KafkaMetadata.OUTGOING_PARTITION, partition);
        }
        if (timestamp >= 0) {
            builder.with(KafkaMetadata.OUTGOING_TIMESTAMP, timestamp);
        }
        if (headers != null) {
            builder.with(KafkaMetadata.OUTGOING_HEADERS, headers.unwrap());
        }
        this.metadata = builder.build();
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
        return (K) metadata.get(KafkaMetadata.OUTGOING_KEY);
    }

    @Override
    public String getTopic() {
        return metadata.getAsString(KafkaMetadata.OUTGOING_TOPIC, null);
    }

    @Override
    public long getTimestamp() {
        return metadata.getAsLong(KafkaMetadata.OUTGOING_TIMESTAMP, -1L);
    }

    @SuppressWarnings("unchecked")
    @Override
    public MessageHeaders getHeaders() {
        Iterable<Header> iterable = metadata.get(KafkaMetadata.OUTGOING_HEADERS, Iterable.class);
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
        return metadata.getAsInteger(KafkaMetadata.OUTGOING_PARTITION, -1);
    }

    @Override
    public long getOffset() {
        // you cannot set the offset on an outgoing message.
        return -1;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }
}
