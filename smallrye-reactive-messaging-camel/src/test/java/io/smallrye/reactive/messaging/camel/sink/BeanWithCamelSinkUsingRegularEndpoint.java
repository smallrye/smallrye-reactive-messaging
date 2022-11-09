package io.smallrye.reactive.messaging.camel.sink;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import io.smallrye.reactive.messaging.camel.OutgoingExchangeMetadata;

@ApplicationScoped
public class BeanWithCamelSinkUsingRegularEndpoint {

    @Outgoing("data")
    public Publisher<Message<String>> source() {
        return ReactiveStreams.of("a", "b", "c", "d")
                .map(String::toUpperCase)
                .map(m -> Message.of(m).addMetadata(
                        new OutgoingExchangeMetadata().putProperty("key", "value").putHeader("headerKey", "headerValue")))
                .buildRs();
    }

}
