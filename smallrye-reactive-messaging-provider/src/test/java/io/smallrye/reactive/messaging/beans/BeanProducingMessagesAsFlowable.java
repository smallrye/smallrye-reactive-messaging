package io.smallrye.reactive.messaging.beans;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;

@ApplicationScoped
public class BeanProducingMessagesAsFlowable {

    @Outgoing("sink")
    public Flowable<Message<String>> publisher() {
        return Flowable.range(1, 10).flatMap(i -> Flowable.just(i, i)).map(i -> Integer.toString(i))
                .map(Message::of);
    }

}
