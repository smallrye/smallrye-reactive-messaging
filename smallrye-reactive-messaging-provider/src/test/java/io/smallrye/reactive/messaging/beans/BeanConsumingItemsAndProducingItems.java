package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class BeanConsumingItemsAndProducingItems {

    @Incoming("count")
    @Outgoing("sink")
    String process(int value) {
        return Integer.toString(value + 1);
    }

}
