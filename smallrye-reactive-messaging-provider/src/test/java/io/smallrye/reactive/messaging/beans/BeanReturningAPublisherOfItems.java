package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanReturningAPublisherOfItems {

    @Outgoing("producer")
    public Flow.Publisher<String> create() {
        return Multi.createFrom().items("a", "b", "c");
    }

}
