package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanProducingMessagesAsPublisher {

    @Outgoing("sink")
    public Flow.Publisher<Message<String>> publisher() {
        return Multi.createFrom()
                .range(1, 11)
                .flatMap(i -> Multi.createFrom().items(i, i))
                .map(i -> Integer.toString(i))
                .map(Message::of);
    }

}
