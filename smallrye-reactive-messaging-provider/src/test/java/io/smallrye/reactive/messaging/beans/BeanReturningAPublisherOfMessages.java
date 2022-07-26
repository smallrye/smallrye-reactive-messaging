package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.Flow.Publisher;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanReturningAPublisherOfMessages {

    @Outgoing("producer")
    public Publisher<Message<String>> create() {
        return AdaptersToFlow.publisher(ReactiveStreams.of("a", "b", "c").map(Message::of).buildRs());
    }

}
