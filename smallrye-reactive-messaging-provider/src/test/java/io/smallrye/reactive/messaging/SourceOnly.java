package io.smallrye.reactive.messaging;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

@ApplicationScoped
public class SourceOnly {

    @Outgoing("count")
    public Publisher<Message<String>> source() {
        return Flowable.range(1, 10)
                .map(i -> Integer.toString(i))
                .map(payload -> Message.<String>newBuilder().payload(payload).build())
                .flatMap(m -> ReactiveStreams.of(m, m).buildRs());
    }

}
