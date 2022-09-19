package io.smallrye.reactive.messaging.camel.source;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.camel.CamelMessage;

@ApplicationScoped
public class BeanWithCamelSourceUsingRegularEndpoint {

    private final List<String> list = new ArrayList<>();

    @Incoming("data")
    public CompletionStage<Void> sink(CamelMessage<String> msg) {
        list.add(msg.getPayload());
        return msg.ack();
    }

    public List<String> list() {
        return list;
    }

}
