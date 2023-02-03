package io.smallrye.reactive.messaging;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class MyBean {

    static final List<String> COLLECTOR = new ArrayList<>();

    @Incoming("my-dummy-stream")
    @Outgoing("toUpperCase")
    public Multi<Message<String>> toUppercase(Multi<Message<String>> input) {
        return input
                .map(Message::getPayload)
                .map(String::toUpperCase)
                .map(Message::of);
    }

    @Incoming("toUpperCase")
    @Outgoing("my-output")
    public PublisherBuilder<Message<String>> duplicate(PublisherBuilder<Message<String>> input) {
        return input.flatMap(s -> ReactiveStreams.of(s.getPayload(), s.getPayload()).map(Message::of));
    }

    @Outgoing("my-dummy-stream")
    Publisher<Message<String>> stream() {
        return Multi.createFrom().items("foo", "bar").map(Message::of);
    }

    @Incoming("my-output")
    Subscriber<Message<String>> output() {
        return ReactiveStreams.<Message<String>> builder().forEach(s -> COLLECTOR.add(s.getPayload())).build();
    }
}
