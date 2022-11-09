package io.smallrye.reactive.messaging.blocking.beans;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.annotations.Blocking;

@ApplicationScoped
public class IncomingBlockingExceptionBean {
    private final List<String> list = new CopyOnWriteArrayList<>();

    @Incoming("in")
    @Blocking
    public void consume(String s) {
        if (s.equals("c")) {
            throw new RuntimeException("KABOOM");
        }
        list.add(s);
    }

    public List<String> list() {
        return list;
    }
}
