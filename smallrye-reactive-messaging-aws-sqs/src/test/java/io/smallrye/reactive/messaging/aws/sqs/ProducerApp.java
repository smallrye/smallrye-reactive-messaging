package io.smallrye.reactive.messaging.aws.sqs;

import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.Dependent;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Uni;

@Dependent
public class ProducerApp {

    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    @Outgoing("data")
    public Uni<String> send() {
        if (COUNTER.getAndIncrement() > 9) {
            return Uni.createFrom().nullItem();
        }
        return Uni.createFrom().item(String.format("hello-%d", COUNTER.get()));
    }
}
