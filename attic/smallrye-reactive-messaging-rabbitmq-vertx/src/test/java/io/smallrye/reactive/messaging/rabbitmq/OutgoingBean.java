package io.smallrye.reactive.messaging.rabbitmq;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

/**
 * A bean that can be registered to do just enough to support the
 * declaration of an exchange backing an outgoing rabbitmq channel.
 */
@ApplicationScoped
public class OutgoingBean {

    @Outgoing("sink")
    public Multi<String> process() {
        return Multi.createFrom().items("test", "test2", "test3");
    }

}
