package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Processor;

import io.reactivex.Flowable;

@ApplicationScoped
public class BeanProducingAProcessorOfMessages {

    @Incoming("count")
    @Outgoing("sink")
    public Processor<Message<Integer>, Message<String>> process() {
        return ReactiveStreams.<Message<Integer>> builder()
                .map(Message::getPayload)
                .map(i -> i + 1)
                .flatMapRsPublisher(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i))
                .map(Message::of)
                .buildRs();
    }

}
