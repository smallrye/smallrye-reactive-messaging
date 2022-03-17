package io.smallrye.reactive.messaging.kafka;

import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.kafka.impl.ce.KafkaCloudEventHelper;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public class IncomingKafkaRecord<K, T> implements KafkaRecord<K, T> {

    private Metadata metadata;
    // TODO add as a normal import once we have removed IncomingKafkaRecordMetadata in this package
    private final io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata<K, T> kafkaMetadata;
    private final KafkaCommitHandler commitHandler;
    private final KafkaFailureHandler onNack;
    private final T payload;

    @SuppressWarnings("deprecation")
    public IncomingKafkaRecord(ConsumerRecord<K, T> record,
            String channel,
            int index,
            KafkaCommitHandler commitHandler,
            KafkaFailureHandler onNack,
            boolean cloudEventEnabled,
            boolean tracingEnabled) {
        this.commitHandler = commitHandler;
        this.kafkaMetadata = new io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata<>(record, channel, index);
        // TODO remove this duplication once we have removed IncomingKafkaRecordMetadata from this package
        // Duplicate the metadata so old and new copies can both be found
        IncomingKafkaRecordMetadata<K, T> deprecatedKafkaMetadata = new IncomingKafkaRecordMetadata<>(record, channel, index);

        ArrayList<Object> meta = new ArrayList<>();
        meta.add(this.kafkaMetadata);
        meta.add(deprecatedKafkaMetadata);
        T payload = null;
        boolean payloadSet = false;
        if (cloudEventEnabled) {
            // Cloud Event detection
            KafkaCloudEventHelper.CloudEventMode mode = KafkaCloudEventHelper.getCloudEventMode(record);
            switch (mode) {
                case NOT_A_CLOUD_EVENT:
                    break;
                case STRUCTURED:
                    CloudEventMetadata<T> event = KafkaCloudEventHelper
                            .createFromStructuredCloudEvent(record);
                    meta.add(event);
                    payloadSet = true;
                    payload = event.getData();
                    break;
                case BINARY:
                    meta.add(KafkaCloudEventHelper.createFromBinaryCloudEvent(record));
                    break;
            }
        }

        this.metadata = ContextAwareMessage.captureContextMetadata(meta);
        this.onNack = onNack;
        if (payload == null && !payloadSet) {
            this.payload = record.value();
        } else {
            this.payload = payload;
        }
    }

    @Override
    public T getPayload() {
        return payload;
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
    public Function<Throwable, CompletionStage<Void>> getNack() {
        return this::nack;
    }

    @Override
    public CompletionStage<Void> ack() {
        return commitHandler.handle(this).subscribeAsCompletionStage();
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        return onNack.handle(this, reason, metadata).subscribeAsCompletionStage();
    }

    public synchronized void injectMetadata(Object metadata) {
        this.metadata = this.metadata.with(metadata);
    }

}
