package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import reactor.core.publisher.Flux;

@ApplicationScoped
public class BeanConsumingItemAsFluxAndPublishingItemAsPublisher {

    @Incoming("count")
    @Outgoing("sink")
    public Publisher<String> process(Flux<Integer> source) {
        return source
                .map(i -> i + 1)
                .flatMap(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i));
    }

}
