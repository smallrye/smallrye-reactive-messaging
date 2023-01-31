package io.smallrye.reactive.messaging.camel.sink;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

@ApplicationScoped
public class BeanWithCamelSinkUsingRSRoute extends RouteBuilder {

    @Outgoing("data")
    public Publisher<Message<String>> source() {
        return ReactiveStreams.of("a", "b", "c", "d")
                .map(String::toUpperCase)
                .map(Message::of)
                .buildRs();
    }

    @Override
    public void configure() {
        from("reactive-streams:in").to("file:./target?fileName=values.txt&fileExist=append");
    }
}
