package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;
import mutiny.zero.flow.adapters.AdaptersToReactiveStreams;

@ApplicationScoped
public class BeanConsumingItemAsPublisherAndPublishingItemAsFlowable {

    @Incoming("count")
    @Outgoing("sink")
    public Flowable<String> process(Flow.Publisher<Integer> source) {
        return Flowable.fromPublisher(AdaptersToReactiveStreams.publisher(source))
                .map(i -> i + 1)
                .flatMap(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i));
    }

}
