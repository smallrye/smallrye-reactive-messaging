package io.smallrye.reactive.messaging.blocking.beans;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.annotations.Blocking;

@ApplicationScoped
public class BeanConsumingItemsAndProducingItems {
    private List<String> threads = new CopyOnWriteArrayList<>();

    @Blocking
    @Incoming("count")
    @Outgoing("sink")
    public String process(int value) {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        threads.add(Thread.currentThread().getName());
        return Integer.toString(value + 1);
    }

    public List<String> threads() {
        return threads;
    }
}
