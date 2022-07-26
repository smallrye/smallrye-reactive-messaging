package io.smallrye.reactive.messaging.inject;

import java.util.concurrent.Flow.Publisher;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class SourceBean {

    @Outgoing("hello")
    @Broadcast
    public Publisher<String> hello() {
        return AdaptersToFlow.publisher(Flowable.fromArray("h", "e", "l", "l", "o"));
    }

    @Outgoing("bonjour")
    @Incoming("raw")
    public Multi<String> bonjour(Multi<String> input) {
        return input.map(String::toUpperCase);
    }

    @Outgoing("raw")
    public Multi<String> raw() {
        return Multi.createFrom().items("b", "o", "n", "j", "o", "u", "r");
    }

}
