package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

@ApplicationScoped
public class BeanConsumingItemAsPublisherAndPublishingItemAsFlowable {

    @Incoming("count")
    @Outgoing("sink")
    public Flowable<String> process(Publisher<Integer> source) {
        return Flowable.fromPublisher(source)
                .map(i -> i + 1)
                .flatMap(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i));
    }

}
