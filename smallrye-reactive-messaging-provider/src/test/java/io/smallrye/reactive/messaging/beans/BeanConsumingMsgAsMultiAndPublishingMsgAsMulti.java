package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanConsumingMsgAsMultiAndPublishingMsgAsMulti {

    @Incoming("count")
    @Outgoing("sink")
    Multi<Message<String>> process(Multi<Message<Integer>> source) {
        return source
                .map(Message::getPayload)
                .map(i -> i + 1)
                .flatMap(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i))
                .map(Message::of);
    }

}
