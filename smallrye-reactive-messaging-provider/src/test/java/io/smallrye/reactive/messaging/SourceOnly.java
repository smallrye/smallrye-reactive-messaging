package io.smallrye.reactive.messaging;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class SourceOnly {

    @SuppressWarnings("unchecked")
    @Outgoing("count")
    public Publisher<Message<String>> source() {
        return Multi.createFrom().range(1, 11)
                .map(i -> Integer.toString(i))
                .map(Message::of)
                .concatMap(m -> ReactiveStreams.of(m, m).buildRs());
    }

}
