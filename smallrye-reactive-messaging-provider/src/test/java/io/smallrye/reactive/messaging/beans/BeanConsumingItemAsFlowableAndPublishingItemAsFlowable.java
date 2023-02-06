package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;

@ApplicationScoped
public class BeanConsumingItemAsFlowableAndPublishingItemAsFlowable {

    @Incoming("count")
    @Outgoing("sink")
    Flowable<String> process(Flowable<Integer> source) {
        return source
                .map(i -> i + 1)
                .flatMap(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i));
    }

}
