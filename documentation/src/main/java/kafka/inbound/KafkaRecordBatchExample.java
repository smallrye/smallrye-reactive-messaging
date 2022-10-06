package kafka.inbound;

import java.time.Instant;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaRecordBatch;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@ApplicationScoped
public class KafkaRecordBatchExample {

    // <code>
    @Incoming("prices")
    public CompletionStage<Void> consumeMessage(KafkaRecordBatch<String, Double> records) {
        for (KafkaRecord<String, Double> record : records) {
            record.getMetadata(IncomingKafkaRecordMetadata.class).ifPresent(metadata -> {
                int partition = metadata.getPartition();
                long offset = metadata.getOffset();
                Instant timestamp = metadata.getTimestamp();
                //... process messages
            });
        }
        // ack will commit the latest offsets (per partition) of the batch.
        return records.ack();
    }

    @Incoming("prices")
    public void consumeRecords(ConsumerRecords<String, Double> records) {
        for (TopicPartition partition : records.partitions()) {
            for (ConsumerRecord<String, Double> record : records.records(partition)) {
                //... process messages
            }
        }
    }
    // </code>

}
