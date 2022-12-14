package amqp.inbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class AmqpPriceConsumer {

    @Incoming("prices")
    public void consume(double price) {
        // process your price.
    }

}
