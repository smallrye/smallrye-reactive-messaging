package io.smallrye.reactive.messaging.camel.source;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.apache.camel.builder.LambdaRouteBuilder;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class BeanWithCamelSourceUsingRegularRoute {

    private final List<String> list = new ArrayList<>();

    @Incoming("data")
    public CompletionStage<Void> sink(Message<String> msg) {
        list.add(msg.getPayload());
        return msg.ack();
    }

    public List<String> list() {
        return list;
    }

    @Produces
    public LambdaRouteBuilder configure() {
        return rb -> rb.from("seda:in").process(exchange -> exchange.getMessage()
                .setBody(exchange.getIn().getBody(String.class).toUpperCase()))
                .to("seda:out");
    }
}
