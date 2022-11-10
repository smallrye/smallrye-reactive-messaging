package pulsar.inbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class Converters {

    // <code>
    @Incoming("topic-a")
    public void consume(org.apache.pulsar.client.api.Message<String> message) {
        String key = message.getKey(); // Can be `null` if the incoming record has no key
        String value = message.getValue(); // Can be `null` if the incoming record has no value
    }
    // </code>

}
