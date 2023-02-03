package io.smallrye.reactive.messaging.blocking.beans;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Blocking;

@ApplicationScoped
public class IncomingUniPayloadBlockingBean {
    private List<String> list = new CopyOnWriteArrayList<>();
    private List<String> threads = new CopyOnWriteArrayList<>();

    @Incoming("in")
    @Blocking
    public Uni<Void> consume(String s) {
        threads.add(Thread.currentThread().getName());
        list.add(s);
        return Uni.createFrom().voidItem();
    }

    public List<String> list() {
        return list;
    }

    public List<String> threads() {
        return threads;
    }
}
