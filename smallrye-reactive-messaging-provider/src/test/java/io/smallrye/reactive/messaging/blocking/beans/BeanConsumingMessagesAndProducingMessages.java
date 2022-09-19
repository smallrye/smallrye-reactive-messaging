package io.smallrye.reactive.messaging.blocking.beans;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.annotations.Blocking;

@ApplicationScoped
public class BeanConsumingMessagesAndProducingMessages {
    private List<String> threads = new CopyOnWriteArrayList<>();

    @Blocking
    @Incoming("count")
    @Outgoing("sink")
    public Message<String> process(Message<Integer> value) {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        threads.add(Thread.currentThread().getName());
        return Message.of(Integer.toString(value.getPayload() + 1));
    }

    public List<String> threads() {
        return threads;
    }
}
