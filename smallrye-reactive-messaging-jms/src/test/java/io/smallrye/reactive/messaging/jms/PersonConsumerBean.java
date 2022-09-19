package io.smallrye.reactive.messaging.jms;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class PersonConsumerBean {

    private final List<Person> list = new CopyOnWriteArrayList<>();

    @Incoming("jms")
    public void consume(Person v) {
        list.add(v);
    }

    List<Person> list() {
        return new ArrayList<>(list);
    }

}
