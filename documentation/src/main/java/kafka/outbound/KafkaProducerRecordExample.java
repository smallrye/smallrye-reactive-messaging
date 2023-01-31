package kafka.outbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class KafkaProducerRecordExample {

    // <code>
    @Outgoing("out")
    public ProducerRecord<String, String> generate() {
        return new ProducerRecord<>("my-topic", "key", "value");
    }
    // </code>

}
