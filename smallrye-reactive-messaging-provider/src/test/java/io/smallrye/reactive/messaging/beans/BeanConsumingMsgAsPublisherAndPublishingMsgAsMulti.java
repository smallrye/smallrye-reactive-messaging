package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanConsumingMsgAsPublisherAndPublishingMsgAsMulti {

    @Incoming("count")
    @Outgoing("sink")
    public Multi<Message<String>> process(Flow.Publisher<Message<Integer>> source) {
        return Multi.createFrom().publisher(source)
                .map(Message::getPayload)
                .map(i -> i + 1)
                .flatMap(i -> AdaptersToFlow.publisher(Flowable.just(i, i)))
                .map(i -> Integer.toString(i))
                .map(Message::of);
    }

}
