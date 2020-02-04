package io.smallrye.reactive.messaging.beans;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;

@ApplicationScoped
public class BeanConsumingMsgAsFlowableAndPublishingMsgAsFlowable {

    @Incoming("count")
    @Outgoing("sink")
    public Flowable<Message<String>> process(Flowable<Message<Integer>> source) {
        return source
                .map(Message::getPayload)
                .map(i -> i + 1)
                .flatMap(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i))
                .map(payload -> Message.<String>newBuilder().payload(payload).build());
    }

}
