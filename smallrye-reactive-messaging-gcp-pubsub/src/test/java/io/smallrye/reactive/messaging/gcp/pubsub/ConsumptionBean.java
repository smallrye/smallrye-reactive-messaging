package io.smallrye.reactive.messaging.gcp.pubsub;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class ConsumptionBean {

    private final List<String> messages = new CopyOnWriteArrayList<>();

    public List<String> getMessages() {
        return messages;
    }

    @Incoming("source")
    public void consume(final String message) {
        messages.add(message);
    }
}
