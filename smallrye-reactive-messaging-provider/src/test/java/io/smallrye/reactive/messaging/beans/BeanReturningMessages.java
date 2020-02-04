package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class BeanReturningMessages {

    private AtomicInteger count = new AtomicInteger();

    @Outgoing("infinite-producer")
    public Message<Integer> create() {
        return Message.<Integer>newBuilder().payload(count.incrementAndGet()).build();
    }

}
