package io.smallrye.reactive.messaging.kafka;

import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;

public class IncomingKafkaRecord<K, T> implements KafkaRecord<K, T> {

    private final Metadata metadata;
    private final IncomingKafkaRecordMetadata<K, T> kafkaMetadata;
    private final KafkaCommitHandler commitHandler;
    private final KafkaFailureHandler onNack;

    public IncomingKafkaRecord(
            KafkaConsumerRecord<K, T> record,
            KafkaCommitHandler commitHandler,
            KafkaFailureHandler onNack) {
        this.kafkaMetadata = new IncomingKafkaRecordMetadata<>(record);
        this.metadata = Metadata.of(this.kafkaMetadata);
        this.commitHandler = commitHandler;
        this.onNack = onNack;
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
        return commitHandler.handle(this);
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason) {
        return onNack.handle(this, reason);
    }
}
