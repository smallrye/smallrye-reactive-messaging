package inbound;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class EventBusPriceConsumer {

    @Incoming("prices")
    public void consume(double price) {
        // process your price.
    }

}
