package rabbitmq.inbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class RabbitMQPriceConsumer {

    @Incoming("prices")
    public void consume(String price) {
        // process your price.
    }

}
