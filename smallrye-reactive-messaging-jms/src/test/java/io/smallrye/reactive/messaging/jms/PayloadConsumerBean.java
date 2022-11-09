package io.smallrye.reactive.messaging.jms;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class PayloadConsumerBean {

    private final List<Integer> list = new CopyOnWriteArrayList<>();

    @Incoming("jms")
    public void consume(int v) {
        list.add(v);
    }

    List<Integer> list() {
        return new ArrayList<>(list);
    }

}
