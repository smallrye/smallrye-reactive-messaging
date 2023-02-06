package io.smallrye.reactive.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.CompletionSubscriber;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Subscriber;

@ApplicationScoped
public class CollectorOnly {

    private final List<Message<String>> result = new ArrayList<>();

    @Incoming("sink")
    public Subscriber<Message<String>> sink() {
        CompletionSubscriber<Message<String>, List<Message<String>>> subscriber = ReactiveStreams.<Message<String>> builder()
                .toList().build();
        subscriber.getCompletion().thenAccept(result::addAll);
        return subscriber;
    }

    public List<String> payloads() {
        return result.stream().map(Message::getPayload).collect(Collectors.toList());
    }

    public List<Message<String>> messages() {
        return result;
    }

}
