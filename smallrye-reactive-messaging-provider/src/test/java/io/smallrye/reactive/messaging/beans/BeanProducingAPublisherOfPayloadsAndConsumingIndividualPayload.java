package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.Flow.Publisher;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import io.reactivex.Flowable;
import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload {

    @Incoming("count")
    @Outgoing("sink")
    public Publisher<String> process(Integer payload) {
        return AdaptersToFlow.publisher(ReactiveStreams.of(payload)
                .map(i -> i + 1)
                .flatMapRsPublisher(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i))
                .buildRs());
    }

}
