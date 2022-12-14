package io.smallrye.reactive.messaging.providers.connectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

@ApplicationScoped
public class DummyBean {

    @Incoming(value = "dummy.source")
    @Outgoing(value = "dummy-sink")
    public ProcessorBuilder<Integer, String> process() {
        return ReactiveStreams.<Integer> builder().map(i -> i * 2)
                .map(i -> Integer.toString(i));
    }

}
