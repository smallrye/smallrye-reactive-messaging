package io.smallrye.reactive.messaging.beans;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class BeanConsumingItemsAndProducingMessages {

    @Incoming("count")
    @Outgoing("sink")
    public Message<String> process(int value) {
        return Message.of(Integer.toString(value + 1));
    }

}
