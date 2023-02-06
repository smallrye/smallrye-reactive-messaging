package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import mutiny.zero.flow.adapters.AdaptersToReactiveStreams;

@ApplicationScoped
public class BeanConsumingItemAsMultiAndPublishingItemAsRSPublisher {

    @Incoming("count")
    @Outgoing("sink")
    public Publisher<String> process(Multi<Integer> source) {
        return Flowable.fromPublisher(AdaptersToReactiveStreams.publisher(source))
                .map(i -> i + 1)
                .map(i -> Integer.toString(i + 1));
    }

}
