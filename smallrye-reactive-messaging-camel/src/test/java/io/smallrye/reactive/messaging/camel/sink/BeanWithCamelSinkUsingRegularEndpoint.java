package io.smallrye.reactive.messaging.camel.sink;

import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import io.smallrye.reactive.messaging.camel.OutgoingExchangeMetadata;
import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanWithCamelSinkUsingRegularEndpoint {

    @Outgoing("data")
    public Flow.Publisher<Message<String>> source() {
        return AdaptersToFlow.publisher(ReactiveStreams.of("a", "b", "c", "d")
                .map(String::toUpperCase)
                .map(m -> Message.of(m).addMetadata(
                        new OutgoingExchangeMetadata().putProperty("key", "value").putHeader("headerKey", "headerValue")))
                .buildRs());
    }

}
