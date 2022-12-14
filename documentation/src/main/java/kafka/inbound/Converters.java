package kafka.inbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.kafka.Record;

@ApplicationScoped
public class Converters {

    // <code>
    @Incoming("topic-a")
    public void consume(Record<String, String> record) {
        String key = record.key(); // Can be `null` if the incoming record has no key
        String value = record.value(); // Can be `null` if the incoming record has no value
    }

    @Incoming("topic-b")
    public void consume(ConsumerRecord<String, String> record) {
        String key = record.key(); // Can be `null` if the incoming record has no key
        String value = record.value(); // Can be `null` if the incoming record has no value
        String topic = record.topic();
        int partition = record.partition();
        // ...
    }
    // </code>

}
