package outbound;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class KafkaProducerRecordExample {

    // tag::code[]
    @Outgoing("out")
    public ProducerRecord<String, String> generate() {
        return new ProducerRecord<>("my-topic", "key", "value");
    }
    // end::code[]

}
