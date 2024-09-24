package kafka.inbound;

import java.util.List;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordBatchMetadata;

@ApplicationScoped
public class KafkaRecordBatchExample {

    // <code>
    @Incoming("prices")
    public CompletionStage<Void> consumeMessage(Message<List<Double>> msg) {
        IncomingKafkaRecordBatchMetadata<String, Double> batchMetadata = msg.getMetadata(IncomingKafkaRecordBatchMetadata.class).get();
        for (ConsumerRecord<String, Double> record : batchMetadata.getRecords()) {
            int partition = record.partition();
            long offset = record.offset();
            long timestamp = record.timestamp();
        }
        // ack will commit the latest offsets (per partition) of the batch.
        return msg.ack();
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

    // <batch>
    @Incoming("prices")
    public void consumeRecords(ConsumerRecords<String, Double> records, IncomingKafkaRecordBatchMetadata<String, Double> metadata) {
        for (TopicPartition partition : records.partitions()) {
            for (ConsumerRecord<String, Double> record : records.records(partition)) {
                TracingMetadata tracing = metadata.getMetadataForRecord(record, TracingMetadata.class);
                if (tracing != null) {
                    tracing.getCurrentContext().makeCurrent();
                }
                //... process messages
            }
        }
    }
    // </batch>

}
