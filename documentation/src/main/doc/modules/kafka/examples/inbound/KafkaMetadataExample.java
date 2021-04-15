package inbound;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.time.Instant;

public class KafkaMetadataExample {


    @SuppressWarnings("unchecked")
    public void metadata() {
        Message<Double> incoming = Message.of(12.0);
        // tag::code[]
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
        // end::code[]
    }

}
