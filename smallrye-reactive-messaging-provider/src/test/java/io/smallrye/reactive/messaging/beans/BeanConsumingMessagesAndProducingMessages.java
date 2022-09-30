package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class BeanConsumingMessagesAndProducingMessages {

    @Incoming("count")
    @Outgoing("sink")
    Message<String> process(Message<Integer> value) {
        return Message.of(Integer.toString(value.getPayload() + 1));
    }

}
