package io.smallrye.reactive.messaging.rabbitmq;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;

/**
 * A bean that can be registered to do just enough to support the
 * declaration of an exchange/queue/etc backing an incoming rabbitmq channel.
 */
@ApplicationScoped
public class IncomingBean {

    @Incoming("data")
    public Uni<Void> process(Message<Integer> input) {
        return Uni.createFrom().voidItem();
    }

}
