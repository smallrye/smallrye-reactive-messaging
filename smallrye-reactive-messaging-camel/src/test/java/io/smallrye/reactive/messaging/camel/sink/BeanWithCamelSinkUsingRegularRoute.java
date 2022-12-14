package io.smallrye.reactive.messaging.camel.sink;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import io.smallrye.reactive.messaging.camel.OutgoingExchangeMetadata;

@ApplicationScoped
public class BeanWithCamelSinkUsingRegularRoute extends RouteBuilder {

    private List<Map<String, Object>> props = new CopyOnWriteArrayList<>();
    private List<Map<String, Object>> headers = new CopyOnWriteArrayList<>();

    @Outgoing("data")
    public Publisher<Message<String>> source() {
        return ReactiveStreams.of("a", "b", "c", "d")
                .map(String::toUpperCase)
                .map(m -> Message.of(m).addMetadata(
                        new OutgoingExchangeMetadata().putProperty("key", "value").putHeader("headerKey", "headerValue")))
                .buildRs();
    }

    @Override
    public void configure() {
        from("seda:in")
                .process(exchange -> props.add(exchange.getProperties()))
                .process(exchange -> headers.add(exchange.getIn().getHeaders()))
                .to("file:./target?fileName=values.txt&fileExist=append");
    }

    public List<Map<String, Object>> getList() {
        return props;
    }

    public List<Map<String, Object>> getHeaders() {
        return headers;
    }
}
