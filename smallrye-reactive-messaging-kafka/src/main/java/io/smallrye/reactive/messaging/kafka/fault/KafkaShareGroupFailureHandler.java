package io.smallrye.reactive.messaging.kafka.fault;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaShareConsumer;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.queues.ShareGroupAcknowledgement;

/**
 * Failure handler for Kafka Share Group consumers that determines the appropriate acknowledgment type
 * based on the provided metadata and acknowledges the message accordingly.
 */
public class KafkaShareGroupFailureHandler implements KafkaFailureHandler {

    private final KafkaShareConsumer<?, ?> consumer;
    private final AcknowledgeType defaultAckType;

    public KafkaShareGroupFailureHandler(KafkaShareConsumer<?, ?> consumer, AcknowledgeType defaultAckType) {
        this.consumer = consumer;
        this.defaultAckType = defaultAckType;
    }

    @Override
    public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record, Throwable reason, Metadata metadata) {
        IncomingKafkaRecordMetadata m = record.getMetadata(IncomingKafkaRecordMetadata.class).get();
        ConsumerRecord rec = m.getRecord();
        // check metadata of type AcknowledgeType
        AcknowledgeType ackType = metadata.get(AcknowledgeType.class)
                // if not found, check for ShareGroupAcknowledgement metadata from nack metadata
                .or(() -> metadata.get(ShareGroupAcknowledgement.class)
                        .map(ShareGroupAcknowledgement::getAcknowledgeType))
                // if not found, check for ShareGroupAcknowledgement metadata on the record
                .or(() -> record.getMetadata(ShareGroupAcknowledgement.class)
                        .map(ShareGroupAcknowledgement::getAcknowledgeType))
                // if not found, use the default ack type
                .orElse(defaultAckType);
        return consumer.acknowledge(rec, ackType)
                .emitOn(record::runOnMessageContext);
    }
}
