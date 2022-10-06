package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Processor;

import io.reactivex.Flowable;

@ApplicationScoped
public class BeanProducingAProcessorOfPayloads {

    @Incoming("count")
    @Outgoing("sink")
    Processor<Integer, String> process() {
        return ReactiveStreams.<Integer> builder()
                .map(i -> i + 1)
                .flatMapRsPublisher(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i))
                .buildRs();
    }

}
