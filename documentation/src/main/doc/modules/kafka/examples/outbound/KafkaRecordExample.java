package outbound;

import io.smallrye.reactive.messaging.kafka.Record;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class KafkaRecordExample {

    // tag::code[]
    @Incoming("in")
    @Outgoing("out")
    public Record<String, String> process(String in) {
        return Record.of("my-key", in);
    }
    // end::code[]

}
