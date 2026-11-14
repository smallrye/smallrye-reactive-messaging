package io.smallrye.reactive.messaging.kafka.converters;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordBatch;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaRecordBatch;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordBatchMetadata;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;

/**
 * Restores the Kafka message type ({@link KafkaRecord}, {@link IncomingKafkaRecord}, {@link KafkaRecordBatch},
 * {@link IncomingKafkaRecordBatch}) for methods consuming it, when the message lost it on the way: features that
 * intercept messages (interceptors, observation, graceful shutdown tracking) rebuild messages, keeping the payload,
 * the metadata and the acknowledgement, but not the connector's message type.
 * <p>
 * The restored message takes its payload and metadata from the intercepted message, and delegates acknowledgement
 * to it, so the interception keeps working.
 * <p>
 * Consuming Kafka message types is deprecated: this is a compatibility measure, and it logs a warning.
 */
@ApplicationScoped
public class KafkaMessageTypeConverter implements MessageConverter {

    private final AtomicBoolean warned = new AtomicBoolean();

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        Type raw = TypeUtils.getRawTypeIfParameterized(target);
        if (isRecordType(raw)) {
            return !(in instanceof IncomingKafkaRecord)
                    && in.getMetadata(IncomingKafkaRecordMetadata.class).isPresent();
        }
        if (isBatchType(raw)) {
            return !(in instanceof IncomingKafkaRecordBatch)
                    && in.getMetadata(IncomingKafkaRecordBatchMetadata.class).isPresent();
        }
        return false;
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        Type raw = TypeUtils.getRawTypeIfParameterized(target);
        if (warned.compareAndSet(false, true)) {
            log.restoringKafkaMessageType(raw.getTypeName());
        }
        if (isBatchType(raw)) {
            return RestoredKafkaRecordBatch.of(in);
        }
        return new RestoredKafkaRecord<>(in);
    }

    private static boolean isRecordType(Type raw) {
        return raw.equals(KafkaRecord.class) || raw.equals(IncomingKafkaRecord.class);
    }

    private static boolean isBatchType(Type raw) {
        return raw.equals(KafkaRecordBatch.class) || raw.equals(IncomingKafkaRecordBatch.class);
    }

    /**
     * A Kafka record restored from an intercepted message: payload and metadata come from that message, and
     * acknowledgement is delegated to it.
     */
    static class RestoredKafkaRecord<K, T> extends IncomingKafkaRecord<K, T> {

        private final Message<?> delegate;

        @SuppressWarnings("unchecked")
        RestoredKafkaRecord(Message<?> delegate) {
            super(delegate.getMetadata(),
                    (IncomingKafkaRecordMetadata<K, T>) delegate.getMetadata(IncomingKafkaRecordMetadata.class)
                            .orElseThrow(() -> new IllegalStateException("No Kafka record metadata")),
                    (T) delegate.getPayload());
            this.delegate = delegate;
        }

        @Override
        public CompletionStage<Void> ack(Metadata metadata) {
            return delegate.ack(metadata);
        }

        @Override
        public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
            return delegate.nack(reason, metadata);
        }
    }

    /**
     * A Kafka record batch restored from an intercepted message: payload and metadata come from that message, the
     * records from its batch metadata, and acknowledgement is delegated to it.
     */
    static class RestoredKafkaRecordBatch<K, T> extends IncomingKafkaRecordBatch<K, T> {

        private final Message<?> delegate;

        private RestoredKafkaRecordBatch(Message<?> delegate, List<KafkaRecord<K, T>> records,
                Map<TopicPartition, KafkaRecord<K, T>> latestOffsetRecords) {
            super(delegate.getMetadata(), records, latestOffsetRecords);
            this.delegate = delegate;
        }

        @SuppressWarnings("unchecked")
        static <K, T> RestoredKafkaRecordBatch<K, T> of(Message<?> delegate) {
            IncomingKafkaRecordBatchMetadata<K, T> batchMetadata = delegate
                    .getMetadata(IncomingKafkaRecordBatchMetadata.class)
                    .orElseThrow(() -> new IllegalStateException("No Kafka batch metadata"));
            List<KafkaRecord<K, T>> records = new ArrayList<>();
            // Records are grouped by partition, in offset order: the last one of each partition has the latest offset.
            Map<TopicPartition, KafkaRecord<K, T>> latest = new LinkedHashMap<>();
            for (Message<?> message : batchMetadata.getBatchedMessages()) {
                KafkaRecord<K, T> record = message instanceof KafkaRecord
                        ? (KafkaRecord<K, T>) message
                        : new RestoredKafkaRecord<>(message);
                records.add(record);
                latest.put(new TopicPartition(record.getTopic(), record.getPartition()), record);
            }
            return new RestoredKafkaRecordBatch<>(delegate, records, latest);
        }

        @SuppressWarnings("unchecked")
        @Override
        public List<T> getPayload() {
            return (List<T>) delegate.getPayload();
        }

        @Override
        public CompletionStage<Void> ack(Metadata metadata) {
            return delegate.ack(metadata);
        }

        @Override
        public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
            return delegate.nack(reason, metadata);
        }
    }
}
