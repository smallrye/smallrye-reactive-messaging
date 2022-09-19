package io.smallrye.reactive.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class MyCollector {

    private final List<Message<String>> result = new CopyOnWriteArrayList<>();
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final AtomicBoolean completed = new AtomicBoolean();

    @SuppressWarnings({ "SubscriberImplementation", "ReactiveStreamsSubscriberImplementation" })
    @Incoming("sink")
    public Subscriber<Message<String>> sink() {
        return new Subscriber<Message<String>>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(20);
            }

            @Override
            public void onNext(Message<String> message) {
                result.add(message);
            }

            @Override
            public void onError(Throwable t) {
                error.set(t);
            }

            @Override
            public void onComplete() {
                completed.set(true);
            }
        };
    }

    @Outgoing("count")
    public Publisher<Message<Integer>> source() {
        return Multi.createFrom().range(0, 10)
                .map(Message::of);
    }

    public List<String> payloads() {
        List<Message<String>> copy = new ArrayList<>(result);
        return copy.stream().map(Message::getPayload).collect(Collectors.toList());
    }

    public List<Message<String>> messages() {
        return result;
    }

    public Throwable getError() {
        return error.get();
    }

    public boolean hasCompleted() {
        return completed.get();
    }

}
