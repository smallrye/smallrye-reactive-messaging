package io.smallrye.reactive.messaging.rabbitmq;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

/**
 * A bean that can be registered to do just enough to support the
 * declaration of an exchange backing an outgoing rabbitmq channel.
 */
@ApplicationScoped
public class OutgoingBean {

    @Outgoing("sink")
    public Message<String> process() {
        return Message.of("test");
    }

}
