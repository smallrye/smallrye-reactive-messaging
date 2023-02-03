package io.smallrye.reactive.messaging.camel.incoming;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanWithCamelSink {

    @Inject
    private CamelContext camel;

    private final List<String> values = new ArrayList<>();

    @Incoming("camel")
    public CompletionStage<Void> sink(String value) {
        values.add(value);
        ProducerTemplate template = camel.createProducerTemplate();
        return template.asyncSendBody("file:./target?fileName=values.txt&fileExist=append", value).thenApply(x -> null);
    }

    @Outgoing("camel")
    public Multi<String> source() {
        return Multi.createFrom().items("a", "b", "c", "d");
    }

    public List<String> values() {
        return values;
    }

}
