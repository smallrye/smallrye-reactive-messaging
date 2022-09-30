package io.smallrye.reactive.messaging.jms;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class RawMessageConsumerBean {

    private final List<IncomingJmsMessage<?>> messages = new CopyOnWriteArrayList<>();

    @Incoming("jms")
    public CompletionStage<Void> consume(IncomingJmsMessage<?> v) {
        messages.add(v);
        return v.ack();
    }

    List<IncomingJmsMessage<?>> messages() {
        return new ArrayList<>(messages);
    }

}
