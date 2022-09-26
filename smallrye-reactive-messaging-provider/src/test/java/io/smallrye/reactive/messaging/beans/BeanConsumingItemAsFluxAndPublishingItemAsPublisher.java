package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;
import mutiny.zero.flow.adapters.AdaptersToFlow;
import reactor.core.publisher.Flux;

@ApplicationScoped
public class BeanConsumingItemAsFluxAndPublishingItemAsPublisher {

    @Incoming("count")
    @Outgoing("sink")
    public Flow.Publisher<String> process(Flux<Integer> source) {
        return AdaptersToFlow.publisher(source
                .map(i -> i + 1)
                .flatMap(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i)));
    }

}
