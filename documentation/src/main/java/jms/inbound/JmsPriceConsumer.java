package jms.inbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class JmsPriceConsumer {

    @Incoming("prices")
    public void consume(double price) {
        // process your price.
    }

}
