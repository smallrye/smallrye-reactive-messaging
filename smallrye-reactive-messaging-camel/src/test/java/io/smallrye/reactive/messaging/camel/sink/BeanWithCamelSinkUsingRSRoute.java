package io.smallrye.reactive.messaging.camel.sink;

import java.util.concurrent.Flow.Publisher;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.apache.camel.builder.LambdaRouteBuilder;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanWithCamelSinkUsingRSRoute {

    @Outgoing("data")
    public Publisher<Message<String>> source() {
        return AdaptersToFlow.publisher(ReactiveStreams.of("a", "b", "c", "d")
                .map(String::toUpperCase)
                .map(Message::of)
                .buildRs());
    }

    @Produces
    public LambdaRouteBuilder configure() {
        return rb -> rb.from("reactive-streams:in").to("file:./target?fileName=values.txt&fileExist=append");
    }
}
