package io.smallrye.reactive.messaging.beans;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class BeanConsumingMessagesAndReturningSomething {

    private List<String> list = new ArrayList<>();

    @Incoming("count")
    String consume(Message<String> message) {
        list.add(message.getPayload());
        return message.getPayload();
    }

    public List<String> payloads() {
        return list;
    }
}
