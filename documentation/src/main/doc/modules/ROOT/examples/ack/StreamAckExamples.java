package ack;

import io.smallrye.mutiny.Multi;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

public class StreamAckExamples {

    // tag::message[]
    @Incoming("in")
    @Outgoing("out")
    public Publisher<Message<String>> transform(Multi<Message<String>> stream) {
        return stream
            .map(message ->
                message.withPayload(message.getPayload().toUpperCase())
            );
    }
    // end::message[]

    // tag::payload[]
    @Incoming("in")
    @Outgoing("out")
    public Publisher<String> transformPayload(Multi<String> stream) {
        return stream
            // The incoming messages are already acknowledged
            .map(String::toUpperCase);
    }
    // end::payload[]
}
