package io.smallrye.reactive.messaging.providers.metrics;

import java.util.Arrays;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

@ApplicationScoped
public class EmitterMetricsTestBean {

    public static final List<String> TEST_MESSAGES = Arrays.asList("foo", "bar", "baz");

    @Inject
    @Channel("source")
    Emitter<String> emitter;

    public void sendMessages() {
        for (String msg : TEST_MESSAGES) {
            emitter.send(msg);
        }
    }

    @Incoming("source")
    @Outgoing("sink")
    public PublisherBuilder<String> duplicate(String input) {
        return ReactiveStreams.of(input, input);
    }

}
