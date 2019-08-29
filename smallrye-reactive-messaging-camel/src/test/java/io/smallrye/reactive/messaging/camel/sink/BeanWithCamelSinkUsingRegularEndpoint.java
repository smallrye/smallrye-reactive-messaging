package io.smallrye.reactive.messaging.camel.sink;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

@ApplicationScoped
public class BeanWithCamelSinkUsingRegularEndpoint {

    @Outgoing("data")
    public Publisher<Message<String>> source() {
        return ReactiveStreams.of("a", "b", "c", "d")
                .map(String::toUpperCase)
                .map(Message::of)
                .buildRs();
    }

}
