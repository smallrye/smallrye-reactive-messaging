package io.smallrye.reactive.messaging.beans;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class BeanConsumingMessagesAndReturningVoid {

    private List<String> list = new ArrayList<>();

    @Incoming("count")
    void consume(Message<String> message) {
        list.add(message.getPayload());
    }

    public List<String> payloads() {
        return list;
    }
}
