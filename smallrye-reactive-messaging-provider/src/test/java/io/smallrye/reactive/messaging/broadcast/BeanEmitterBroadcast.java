package io.smallrye.reactive.messaging.broadcast;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.annotations.Broadcast;

@ApplicationScoped
public class BeanEmitterBroadcast {
    @Inject
    @Broadcast
    @Channel("X")
    Emitter<String> emitter;

    private final List<String> list = new CopyOnWriteArrayList<>();

    public Emitter<String> emitter() {
        return emitter;
    }

    public List<String> list() {
        return list;
    }

    @Incoming("X")
    public void consume(final String s) {
        list.add(s);
    }

    public void send(String s) {
        emitter.send(s);
    }
}
