package io.smallrye.reactive.messaging;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import mutiny.zero.flow.adapters.AdaptersToFlow;
import mutiny.zero.flow.adapters.AdaptersToReactiveStreams;

@ApplicationScoped
public class SourceOnly {

    @SuppressWarnings("unchecked")
    @Outgoing("count")
    public Publisher<Message<String>> source() {
        return AdaptersToReactiveStreams.publisher(Multi.createFrom().range(1, 11)
                .map(i -> Integer.toString(i))
                .map(Message::of)
                .concatMap(m -> AdaptersToFlow.publisher(ReactiveStreams.of(m, m).buildRs())));
    }

}
