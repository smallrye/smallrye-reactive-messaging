package ack;

import java.util.concurrent.Flow.Publisher;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

public class StreamAckExamples {

    // <message>
    @Incoming("in")
    @Outgoing("out")
    public Publisher<Message<String>> transform(Multi<Message<String>> stream) {
        return stream
                .map(message -> message.withPayload(message.getPayload().toUpperCase()));
    }
    // </message>

    // <payload>
    @Incoming("in")
    @Outgoing("out")
    public Publisher<String> transformPayload(Multi<String> stream) {
        return stream
                // The incoming messages are already acknowledged
                .map(String::toUpperCase);
    }
    // </payload>
}
