package io.smallrye.reactive.messaging.camel.source;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;

import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.camel.CamelMessage;

@ApplicationScoped
public class BeanWithCamelSourceUsingRSEndpoint extends RouteBuilder {

    private final List<String> list = new ArrayList<>();

    @Incoming("data")
    public CompletionStage<Void> sink(CamelMessage<String> msg) {
        list.add(msg.getPayload());
        return msg.ack();
    }

    public List<String> list() {
        return list;
    }

    @Override
    public void configure() {
        from("seda:in").process(exchange -> exchange.getMessage()
                .setBody(exchange.getIn().getBody(String.class).toUpperCase()))
                .to("reactive-streams:out");
    }
}
