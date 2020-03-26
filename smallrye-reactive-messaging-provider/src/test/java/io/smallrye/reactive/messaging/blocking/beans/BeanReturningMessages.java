package io.smallrye.reactive.messaging.blocking.beans;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.annotations.Blocking;

@ApplicationScoped
public class BeanReturningMessages {
    private AtomicInteger count = new AtomicInteger();
    private List<String> threads = new CopyOnWriteArrayList<>();

    @Blocking
    @Outgoing("infinite-producer")
    public Message<Integer> create() {
        threads.add(Thread.currentThread().getName());
        return Message.of(count.incrementAndGet());
    }

    public List<String> threads() {
        return threads;
    }
}
