package kafka.outbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.kafka.Record;

@ApplicationScoped
public class KafkaRecordExample {

    // <code>
    @Incoming("in")
    @Outgoing("out")
    public Record<String, String> process(String in) {
        return Record.of("my-key", in);
    }
    // </code>

}
