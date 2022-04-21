package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class BeanConsumingItemAndProducingCompletionStageOfMessage {

    @Incoming("count")
    @Outgoing("sink")
    public CompletionStage<Message<String>> process(int value) {
        return CompletableFuture.supplyAsync(() -> Integer.toString(value + 1))
                .thenApply(Message::of);
    }
}
