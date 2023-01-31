package io.smallrye.reactive.messaging.blocking.beans;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.annotations.Blocking;

@ApplicationScoped
public class BeanReturningPayloads {
    private AtomicInteger count = new AtomicInteger();
    private List<String> threads = new CopyOnWriteArrayList<>();

    @Blocking
    @Outgoing("infinite-producer")
    public int create() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        threads.add(Thread.currentThread().getName());
        return count.incrementAndGet();
    }

    public List<String> threads() {
        return threads;
    }
}
