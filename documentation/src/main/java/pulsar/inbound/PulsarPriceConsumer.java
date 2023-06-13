package pulsar.inbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class PulsarPriceConsumer {

    @Incoming("prices")
    public void consume(double price) {
        // process your price.
    }

}
