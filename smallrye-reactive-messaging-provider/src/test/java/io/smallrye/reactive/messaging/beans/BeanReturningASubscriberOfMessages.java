package io.smallrye.reactive.messaging.beans;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Subscriber;

@ApplicationScoped
public class BeanReturningASubscriberOfMessages {

    private List<String> list = new ArrayList<>();

    @Incoming("count")
    Subscriber<Message<String>> create() {
        return ReactiveStreams.<Message<String>> builder().forEach(m -> list.add(m.getPayload()))
                .build();
    }

    public List<String> payloads() {
        return list;
    }

}
