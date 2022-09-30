package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

@ApplicationScoped
public class BeanReturningAPublisherBuilderOfItems {

    @Outgoing("producer")
    public Publisher<String> create() {
        return ReactiveStreams.of("a", "b", "c")
                .buildRs();
    }

}
