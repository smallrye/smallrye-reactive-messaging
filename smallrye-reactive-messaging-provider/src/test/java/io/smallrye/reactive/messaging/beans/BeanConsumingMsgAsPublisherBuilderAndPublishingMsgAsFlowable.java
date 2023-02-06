package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;

import io.reactivex.Flowable;

@ApplicationScoped
public class BeanConsumingMsgAsPublisherBuilderAndPublishingMsgAsFlowable {

    @Incoming("count")
    @Outgoing("sink")
    public Flowable<Message<String>> process(PublisherBuilder<Message<Integer>> source) {
        return Flowable.fromPublisher(source.buildRs())
                .map(Message::getPayload)
                .map(i -> i + 1)
                .flatMap(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i))
                .map(Message::of);
    }

}
