package pulsar.outbound;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.pulsar.OutgoingMessage;

public class PulsarOutgoingMessageExample {

    // <code>
    @Incoming("in")
    @Outgoing("out")
    OutgoingMessage<Long> process(org.apache.pulsar.client.api.Message<String> in) {
        return OutgoingMessage.from(in)
                .withValue(Long.valueOf(in.getValue()));
    }
    // </code>
}
