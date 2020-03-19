package examples.kafka;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.kafka.KafkaRecord;

public class KafkaProducer {

    // tag::kafka-message[]
    @Outgoing("to-kafka")
    public Message<String> produce(Message<String> incoming) {
        return KafkaRecord.of("topic", "key", incoming.getPayload());
    }
    // end::kafka-message[]

}
