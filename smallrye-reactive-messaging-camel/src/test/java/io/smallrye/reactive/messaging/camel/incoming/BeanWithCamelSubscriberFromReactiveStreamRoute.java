package io.smallrye.reactive.messaging.camel.incoming;

import java.util.concurrent.Flow.Subscriber;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import org.apache.camel.builder.LambdaRouteBuilder;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreamsService;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;
import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanWithCamelSubscriberFromReactiveStreamRoute {

    @Inject
    private CamelReactiveStreamsService reactive;

    @Incoming("camel")
    public Subscriber<String> sink() {
        return AdaptersToFlow.subscriber(reactive.streamSubscriber("camel-sub", String.class));
    }

    @Outgoing("camel")
    public Multi<String> source() {
        return Multi.createFrom().items("a", "b", "c", "d");
    }

    @Produces
    public LambdaRouteBuilder route() {
        return rb -> rb.from("reactive-streams:camel-sub")
                .to("file:./target?fileName=values.txt&fileExist=append");
    }
}
