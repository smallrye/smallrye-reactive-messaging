package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;

import io.reactivex.Flowable;

@ApplicationScoped
public class BeanConsumingItemAsPublisherBuilderAndPublishingItemAsPublisherBuilder {

    @Incoming("count")
    @Outgoing("sink")
    public PublisherBuilder<String> process(PublisherBuilder<Integer> source) {
        return source
                .map(i -> i + 1)
                .flatMapRsPublisher(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i));
    }

}
