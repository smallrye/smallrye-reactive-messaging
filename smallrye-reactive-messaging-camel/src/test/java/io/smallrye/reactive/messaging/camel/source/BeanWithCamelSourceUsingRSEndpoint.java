package io.smallrye.reactive.messaging.camel.source;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.apache.camel.builder.LambdaRouteBuilder;
import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.camel.CamelMessage;
import io.smallrye.reactive.messaging.camel.IncomingExchangeMetadata;

@ApplicationScoped
public class BeanWithCamelSourceUsingRSEndpoint {

    private final List<String> list = new ArrayList<>();
    private final List<String> props = new ArrayList<>();
    private final List<String> headers = new ArrayList<>();

    @Incoming("data")
    public CompletionStage<Void> sink(CamelMessage<String> msg) {
        IncomingExchangeMetadata metadata = msg.getMetadata(IncomingExchangeMetadata.class).orElse(null);
        Assertions.assertThat(metadata).isNotNull();
        props.add(metadata.getExchange().getProperty("key", String.class));
        headers.add(metadata.getExchange().getIn().getHeader("headerKey", String.class));
        list.add(msg.getPayload());
        return msg.ack();
    }

    public List<String> list() {
        return list;
    }

    public List<String> headers() {
        return headers;
    }

    public List<String> props() {
        return props;
    }

    @Produces
    public LambdaRouteBuilder route() {
        return rb -> rb.from("seda:in").process(exchange -> {
            exchange.getMessage()
                    .setBody(exchange.getIn().getBody(String.class).toUpperCase());
            exchange.setProperty("key", "value");
            exchange.getIn().setHeader("headerKey", "headerValue");
        })
                .to("reactive-streams:out");
    }
}
