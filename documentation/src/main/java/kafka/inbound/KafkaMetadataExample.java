package kafka.inbound;

import java.time.Instant;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

public class KafkaMetadataExample {

    @SuppressWarnings("unchecked")
    public void metadata() {
        Message<Double> incoming = Message.of(12.0);
        // <code>
        IncomingKafkaRecordMetadata<String, Double> metadata = incoming.getMetadata(IncomingKafkaRecordMetadata.class)
                .orElse(null);
        if (metadata != null) {
            // The topic
            String topic = metadata.getTopic();

            // The key
            String key = metadata.getKey();

            // The timestamp
            Instant timestamp = metadata.getTimestamp();

            // The underlying record
            ConsumerRecord<String, Double> record = metadata.getRecord();

            // ...
        }
        // </code>
    }

}
