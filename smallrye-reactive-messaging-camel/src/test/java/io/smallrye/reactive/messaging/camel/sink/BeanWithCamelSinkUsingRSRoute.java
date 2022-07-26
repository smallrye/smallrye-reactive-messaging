package io.smallrye.reactive.messaging.camel.sink;

import java.util.concurrent.Flow.Publisher;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanWithCamelSinkUsingRSRoute extends RouteBuilder {

    @Outgoing("data")
    public Publisher<Message<String>> source() {
        return AdaptersToFlow.publisher(ReactiveStreams.of("a", "b", "c", "d")
                .map(String::toUpperCase)
                .map(Message::of)
                .buildRs());
    }

    @Override
    public void configure() {
        from("reactive-streams:in").to("file:./target?fileName=values.txt&fileExist=append");
    }
}
