package io.smallrye.reactive.messaging.kafka;

import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.grpc.Context;
import io.opentelemetry.OpenTelemetry;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.kafka.impl.ce.KafkaCloudEventHelper;
import io.smallrye.reactive.messaging.kafka.tracing.HeaderExtractAdapter;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;

public class IncomingKafkaRecord<K, T> implements KafkaRecord<K, T> {

    private Metadata metadata;
    private final IncomingKafkaRecordMetadata<K, T> kafkaMetadata;
    private final KafkaCommitHandler commitHandler;
    private final KafkaFailureHandler onNack;
    private final T payload;

    public IncomingKafkaRecord(KafkaConsumerRecord<K, T> record,
            KafkaCommitHandler commitHandler,
            KafkaFailureHandler onNack,
            boolean cloudEventEnabled,
            boolean tracingEnabled) {
        this.commitHandler = commitHandler;
        this.kafkaMetadata = new IncomingKafkaRecordMetadata<>(record);

        ArrayList<Object> meta = new ArrayList<>();
        meta.add(this.kafkaMetadata);
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

        if (tracingEnabled) {
            TracingMetadata tracingMetadata = TracingMetadata.empty();
            if (record.headers() != null) {
                // Read tracing headers
                Context context = OpenTelemetry.getPropagators().getTextMapPropagator()
                        .extract(Context.current(), kafkaMetadata.getHeaders(), HeaderExtractAdapter.GETTER);
                tracingMetadata = TracingMetadata.withPrevious(context);
            }

            meta.add(tracingMetadata);
        }

        this.metadata = Metadata.from(meta);
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
    public CompletionStage<Void> ack() {
        return commitHandler.handle(this);
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason) {
        return onNack.handle(this, reason);
    }

    public synchronized void injectTracingMetadata(TracingMetadata tracingMetadata) {
        metadata = metadata.with(tracingMetadata);
    }
}
