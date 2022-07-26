package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.Flow;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanReturningAPublisherOfItems {

    @Outgoing("producer")
    public Flow.Publisher<String> create() {
        return AdaptersToFlow.publisher(ReactiveStreams.of("a", "b", "c").buildRs());
    }

}
