package io.smallrye.reactive.messaging.beans;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Subscriber;

@ApplicationScoped
public class BeanReturningASubscriberOfPayloads {

    private List<String> list = new ArrayList<>();

    @Incoming("count")
    public Subscriber<String> create() {
        return ReactiveStreams.<String> builder().forEach(m -> list.add(m)).build();
    }

    public List<String> payloads() {
        return list;
    }

}
