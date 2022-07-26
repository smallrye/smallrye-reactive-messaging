package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.Flow.Publisher;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;
import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanProducingPayloadAsPublisher {

    @Outgoing("sink")
    Publisher<String> publisher() {
        return AdaptersToFlow.publisher(Flowable.range(1, 10).flatMap(i -> Flowable.just(i, i)).map(i -> Integer.toString(i)));
    }

}
