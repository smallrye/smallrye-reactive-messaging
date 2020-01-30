package io.smallrye.reactive.messaging.kafka;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;

public class IncomingKafkaRecord<K, T> implements KafkaRecord<K, T> {

    private final KafkaConsumer<K, T> consumer;
    private final Metadata metadata;
    private final IncomingKafkaRecordMetadata<K, T> kafkaMetadata;

    public IncomingKafkaRecord(KafkaConsumer<K, T> consumer, KafkaConsumerRecord<K, T> record) {
        this.consumer = consumer;
        this.kafkaMetadata = new IncomingKafkaRecordMetadata<>(record);
        this.metadata = Metadata.of(this.kafkaMetadata);
    }

    @Override
    public T getPayload() {
        return kafkaMetadata.getRecord().value();
    }

    @Override
    public K getKey() {
        return kafkaMetadata.getKey();
    }

    @Override
    public String getTopic() {
        return kafkaMetadata.getTopic();
    }

    @Override
    public int getPartition() {
        return kafkaMetadata.getPartition();
    }

    @Override
    public Instant getTimestamp() {
        return kafkaMetadata.getTimestamp();
    }

    @Override
    public Headers getHeaders() {
        return kafkaMetadata.getHeaders();
    }

    public long getOffset() {
        return kafkaMetadata.getOffset();
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return this::ack;
    }

    @Override
    public CompletionStage<Void> ack() {
        consumer.commit();
        return CompletableFuture.completedFuture(null);
    }
}
