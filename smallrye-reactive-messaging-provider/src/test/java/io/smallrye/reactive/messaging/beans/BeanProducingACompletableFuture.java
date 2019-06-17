package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.CompletableFuture;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class BeanProducingACompletableFuture {

    @Incoming("count")
    @Outgoing("sink")
    public CompletableFuture<String> process(int value) {
        return CompletableFuture.supplyAsync(() -> Integer.toString(value + 1));
    }
}
