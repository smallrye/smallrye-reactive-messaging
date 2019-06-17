package io.smallrye.reactive.messaging.camel.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.camel.CamelConnector;
import io.smallrye.reactive.messaging.camel.MapBasedConfig;

@ApplicationScoped
public class BeanWithCamelSourceUsingRegularRoute extends RouteBuilder {

    private List<String> list = new ArrayList<>();

    @Incoming("data")
    public CompletionStage<Void> sink(Message<String> msg) {
        list.add(msg.getPayload());
        return msg.ack();
    }

    public List<String> list() {
        return list;
    }

    @Produces
    public Config myConfig() {
        String prefix = "mp.messaging.incoming.data.";
        Map<String, Object> config = new HashMap<>();
        config.putIfAbsent(prefix + "endpoint-uri", "seda:out");
        config.put(prefix + "connector", CamelConnector.CONNECTOR_NAME);
        return new MapBasedConfig(config);
    }

    @Override
    public void configure() {
        from("seda:in").process(exchange -> exchange.getOut().setBody(exchange.getIn().getBody(String.class).toUpperCase()))
                .to("seda:out");
    }
}
