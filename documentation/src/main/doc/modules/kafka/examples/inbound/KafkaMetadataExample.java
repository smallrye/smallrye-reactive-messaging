package inbound;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.time.Instant;
import java.util.Optional;

public class KafkaMetadataExample {


    public void metadata() {
        Message<Double> incoming = Message.of(12.0);
        // tag::code[]
        Optional<IncomingKafkaRecordMetadata<String, Double>> metadata = incoming.getMetadata(IncomingKafkaRecordMetadata.class);
        metadata.ifPresent(meta -> {
            // The topic
            String topic = meta.getTopic();

            // The key
            String key = meta.getKey();

            // The timestamp
            Instant timestamp = meta.getTimestamp();

            // The underlying record
            KafkaConsumerRecord<String, Double> record = meta.getRecord();

            // ...
        });
        // end::code[]
    }

}
