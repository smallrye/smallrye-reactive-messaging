package io.smallrye.reactive.messaging.camel.incoming;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;

@ApplicationScoped
public class BeanWithCamelSink {

    @Inject
    private CamelContext camel;

    private List<String> values = new ArrayList<>();

    @Incoming("camel")
    public CompletionStage<Void> sink(String value) {
        values.add(value);
        ProducerTemplate template = camel.createProducerTemplate();
        return template.asyncSendBody("file:./target?fileName=values.txt&fileExist=append", value).thenApply(x -> null);
    }

    @Outgoing("camel")
    public Flowable<String> source() {
        return Flowable.fromArray("a", "b", "c", "d");
    }

    public List<String> values() {
        return values;
    }

}
