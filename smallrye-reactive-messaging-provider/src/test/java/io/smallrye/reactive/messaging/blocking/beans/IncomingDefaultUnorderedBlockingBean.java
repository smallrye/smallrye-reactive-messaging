package io.smallrye.reactive.messaging.blocking.beans;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.annotations.Blocking;

@ApplicationScoped
public class IncomingDefaultUnorderedBlockingBean {
    private List<String> list = new CopyOnWriteArrayList<>();
    private List<String> threads = new CopyOnWriteArrayList<>();

    @Incoming("in")
    @Blocking(ordered = false)
    public void consume(String s) {
        if (s.equals("a") || s.equals("c") || s.equals("e")) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        threads.add(Thread.currentThread().getName());
        list.add(s);
    }

    public List<String> list() {
        return list;
    }

    public List<String> threads() {
        return threads;
    }
}
