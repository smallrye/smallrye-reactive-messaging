package kafka.inbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordBatchMetadata;
import io.smallrye.reactive.messaging.kafka.queues.ShareGroupAcknowledgement;

@ApplicationScoped
public class KafkaShareGroupBatchAckExample {

    // <code>
    @Incoming("queue")
    public void consume(ConsumerRecords<String, Double> records,
            IncomingKafkaRecordBatchMetadata<String, Double> batchMetadata) {
        for (ConsumerRecord<String, Double> record : records) {
            ShareGroupAcknowledgement ack = batchMetadata.getMetadataForRecord(record, ShareGroupAcknowledgement.class);
            if (record.value() < 0) {
                ack.reject(); // Reject invalid records
            } else {
                ack.accept(); // Accept valid records
            }
        }
    }
    // </code>

}
