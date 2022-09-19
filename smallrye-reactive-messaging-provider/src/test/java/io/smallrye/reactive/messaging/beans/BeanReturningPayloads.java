package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class BeanReturningPayloads {

    private AtomicInteger count = new AtomicInteger();

    @Outgoing("infinite-producer")
    public int create() {
        return count.incrementAndGet();
    }

}
