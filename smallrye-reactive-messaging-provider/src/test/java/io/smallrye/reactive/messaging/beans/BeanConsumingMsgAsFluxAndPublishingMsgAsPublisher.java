package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import reactor.core.publisher.Flux;

@ApplicationScoped
public class BeanConsumingMsgAsFluxAndPublishingMsgAsPublisher {

    @Incoming("count")
    @Outgoing("sink")
    public Publisher<Message<String>> process(Flux<Message<Integer>> source) {
        return source
                .map(Message::getPayload)
                .map(i -> i + 1)
                .flatMap(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i))
                .map(Message::of);
    }

}
