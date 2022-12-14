package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class BeanProducingACompletionStage {

    @Incoming("count")
    @Outgoing("sink")
    public CompletionStage<String> process(int value) {
        return CompletableFuture.supplyAsync(() -> Integer.toString(value + 1));
    }
}
