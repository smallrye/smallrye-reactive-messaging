package io.smallrye.reactive.messaging.beans;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class BeanConsumingMessagesAndProducingItems {

    @Incoming("count")
    @Outgoing("sink")
    public String process(Message<Integer> value) {
        return Integer.toString(value.getPayload() + 1);
    }

}
