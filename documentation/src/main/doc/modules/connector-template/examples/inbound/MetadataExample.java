package inbound;

import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.time.Instant;
import java.util.Optional;

public class MetadataExample {


    public void metadata() {
        Message<Double> incoming = Message.of(12.0);
        // tag::code[]

        //TASK - Change class name
        Optional<IncomingKafkaRecordMetadata<String, Double>> metadata = incoming.getMetadata(IncomingKafkaRecordMetadata.class);
        metadata.ifPresent(meta -> {
            //TASK - Show usage

        });
        // end::code[]
    }

}
