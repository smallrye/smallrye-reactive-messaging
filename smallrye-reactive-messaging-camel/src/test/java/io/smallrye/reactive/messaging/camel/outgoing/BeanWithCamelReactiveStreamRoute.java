package io.smallrye.reactive.messaging.camel.outgoing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreamsService;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

@ApplicationScoped
public class BeanWithCamelReactiveStreamRoute extends RouteBuilder {

    @Inject
    private CamelReactiveStreamsService camel;

    private final List<String> values = new ArrayList<>();

    @Incoming("sink")
    public CompletionStage<Void> sink(String value) {
        values.add(value);
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("camel")
    @Outgoing("sink")
    public String extract(Exchange exchange) {
        return exchange.getIn().getBody(String.class);
    }

    @Outgoing("camel")
    public Publisher<Exchange> source() {
        return camel.fromStream("my-stream");
    }

    public List<String> values() {
        return values;
    }

    @Override
    public void configure() {
        from("seda:camel")
                .process(exchange -> exchange.getMessage().setBody(exchange.getIn().getBody(String.class).toUpperCase()))
                .to("reactive-streams:my-stream");
    }
}
