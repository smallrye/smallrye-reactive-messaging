package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.CompletableFuture;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class BeanProducingACompletableFutureOfMessage {

    @Incoming("count")
    @Outgoing("sink")
    public CompletableFuture<Message<String>> process(Message<Integer> value) {
        return CompletableFuture.supplyAsync(() -> Integer.toString(value.getPayload() + 1))
                .thenApply(Message::of);
    }
}
