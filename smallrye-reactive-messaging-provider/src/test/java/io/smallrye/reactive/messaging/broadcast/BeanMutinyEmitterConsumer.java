package io.smallrye.reactive.messaging.broadcast;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class BeanMutinyEmitterConsumer {
    private final List<String> list = new CopyOnWriteArrayList<>();

    public List<String> list() {
        return list;
    }

    @Incoming("X")
    public void consume(final String s) {
        list.add(s);
    }
}
