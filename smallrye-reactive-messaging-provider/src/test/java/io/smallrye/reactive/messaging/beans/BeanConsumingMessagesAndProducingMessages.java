package io.smallrye.reactive.messaging.beans;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class BeanConsumingMessagesAndProducingMessages {

    @Incoming("count")
    @Outgoing("sink")
    public Message<String> process(Message<Integer> value) {
        return Message.<String>newBuilder().payload(Integer.toString(value.getPayload() + 1)).build();
    }

}
