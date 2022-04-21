package io.smallrye.reactive.messaging.eventbus;

import java.util.ArrayList;
import java.util.List;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
public class ProducingBean {

    @Inject
    Vertx vertx;
    private final List<io.vertx.mutiny.core.eventbus.Message<?>> messages = new ArrayList<>();

    @Incoming("data")
    @Outgoing("sink")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message<Integer> process(Message<Integer> input) {
        return Message.of(input.getPayload() + 1, input::ack);
    }

    // As we can't use the Usage class - not the same Vert.x instance, receive the message here.

    @Outgoing("data")
    public Publisher<Integer> source() {
        return Multi.createFrom().range(0, 10);
    }

    @PostConstruct
    public void registerConsumer() {
        vertx.eventBus().consumer("sink").handler(messages::add);
    }

    public List<io.vertx.mutiny.core.eventbus.Message<?>> messages() {
        return messages;
    }

}
