package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanReturningAPublisherOfMessages {

    @Outgoing("producer")
    public Flow.Publisher<Message<String>> create() {
        return Multi.createFrom().items("a", "b", "c").map(Message::of);
    }

}
