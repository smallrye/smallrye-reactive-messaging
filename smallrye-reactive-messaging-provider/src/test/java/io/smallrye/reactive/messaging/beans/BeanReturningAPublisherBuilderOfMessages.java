package io.smallrye.reactive.messaging.beans;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

@ApplicationScoped
public class BeanReturningAPublisherBuilderOfMessages {

    @Outgoing("producer")
    public PublisherBuilder<Message<String>> create() {
        return ReactiveStreams.of("a", "b", "c").map(Message::of);
    }

}
