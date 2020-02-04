package io.smallrye.reactive.messaging.beans;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

@ApplicationScoped
public class BeanReturningAPublisherOfMessages {

    @Outgoing("producer")
    public Publisher<Message<String>> create() {
        return ReactiveStreams.of("a", "b", "c").map(payload -> Message.<String>newBuilder().payload(payload).build()).buildRs();
    }

}
