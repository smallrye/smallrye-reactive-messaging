package io.smallrye.reactive.messaging.inject;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.annotations.Broadcast;

@ApplicationScoped
public class SourceBean {

    @Outgoing("hello")
    @Broadcast
    public Publisher<String> hello() {
        return Flowable.fromArray("h", "e", "l", "l", "o");
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
