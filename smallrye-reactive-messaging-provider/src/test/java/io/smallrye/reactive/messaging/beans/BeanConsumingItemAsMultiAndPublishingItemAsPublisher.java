package io.smallrye.reactive.messaging.beans;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import mutiny.zero.flow.adapters.AdaptersToFlow;
import mutiny.zero.flow.adapters.AdaptersToReactiveStreams;

@ApplicationScoped
public class BeanConsumingItemAsMultiAndPublishingItemAsPublisher {

    @Incoming("count")
    @Outgoing("sink")
    public Publisher<String> process(Multi<Integer> source) {
        return AdaptersToReactiveStreams.publisher(source
                .map(i -> i + 1)
                .flatMap(i -> AdaptersToFlow.publisher(Flowable.just(i, i)))
                .map(i -> Integer.toString(i)));
    }

}
