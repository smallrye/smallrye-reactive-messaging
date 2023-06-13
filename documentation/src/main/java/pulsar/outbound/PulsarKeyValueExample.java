package pulsar.outbound;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
public class PulsarKeyValueExample {

    // <code>
    @Identifier("out")
    @Produces
    Schema<KeyValue<String, Long>> schema = Schema.KeyValue(Schema.STRING, Schema.INT64);

    @Incoming("in")
    @Outgoing("out")
    public KeyValue<String, Long> process(long in) {
        return new KeyValue<>("my-key", in);
    }
    // </code>

}
