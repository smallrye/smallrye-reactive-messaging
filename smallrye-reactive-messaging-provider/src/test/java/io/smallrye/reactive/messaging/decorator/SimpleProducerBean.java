package io.smallrye.reactive.messaging.decorator;

import java.util.Arrays;
import java.util.List;

import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

public class SimpleProducerBean {

    public static final List<String> TEST_STRINGS = Arrays.asList("foo", "bar", "baz");

    @Outgoing("sink")
    public PublisherBuilder<String> createStrings() {
        return ReactiveStreams.fromIterable(TEST_STRINGS);
    }

}
