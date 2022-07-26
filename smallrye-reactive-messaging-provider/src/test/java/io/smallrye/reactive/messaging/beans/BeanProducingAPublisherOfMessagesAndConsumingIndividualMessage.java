package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.Flow;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import io.reactivex.Flowable;
import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanProducingAPublisherOfMessagesAndConsumingIndividualMessage {

    @Incoming("count")
    @Outgoing("sink")
    public Flow.Publisher<Message<String>> process(Message<Integer> message) {
        return AdaptersToFlow.publisher(ReactiveStreams.of(message)
                .map(Message::getPayload)
                .map(i -> i + 1)
                .flatMapRsPublisher(i -> Flowable.just(i, i))
                .map(i -> Integer.toString(i))
                .map(Message::of)
                .buildRs());
    }

}
