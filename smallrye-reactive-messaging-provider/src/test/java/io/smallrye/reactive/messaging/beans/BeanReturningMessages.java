package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class BeanReturningMessages {

    private AtomicInteger count = new AtomicInteger();

    @Outgoing("infinite-producer")
    Message<Integer> create() {
        return Message.of(count.incrementAndGet());
    }

}
