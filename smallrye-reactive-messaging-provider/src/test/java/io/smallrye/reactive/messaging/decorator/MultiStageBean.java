package io.smallrye.reactive.messaging.decorator;

import java.util.Arrays;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

@ApplicationScoped
public class MultiStageBean {

    public static final List<String> TEST_STRINGS = Arrays.asList("foo", "bar", "baz");

    @Outgoing("A")
    public PublisherBuilder<String> produceTestStrings() {
        return ReactiveStreams.fromIterable(TEST_STRINGS);
    }

    @Incoming("A")
    @Outgoing("B")
    public PublisherBuilder<String> doubleUp(String input) {
        return ReactiveStreams.of(input, input);
    }

    @Incoming("B")
    @Outgoing("sink")
    public String doNothing(String input) {
        return input;
    }

}
