package inbound;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class RabbitMQPriceConsumer {

    @Incoming("prices")
    public void consume(String price) {
        // process your price.
    }

}
