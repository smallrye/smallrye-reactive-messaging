package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;
import mutiny.zero.flow.adapters.AdaptersToFlow;
import reactor.core.publisher.Flux;

@ApplicationScoped
public class BeanConsumingMsgAsFluxAndPublishingMsgAsPublisher {

    @Incoming("count")
    @Outgoing("sink")
    public Flow.Publisher<Message<String>> process(Flux<Message<Integer>> source) {
        return AdaptersToFlow.publisher(source
                .map(Message::getPayload)
                .map(i -> i + 1)
                .flatMap(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i))
                .map(Message::of));
    }

}
