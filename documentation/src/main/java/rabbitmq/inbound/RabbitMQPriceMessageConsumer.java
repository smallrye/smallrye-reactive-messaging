package rabbitmq.inbound;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class RabbitMQPriceMessageConsumer {

    @Incoming("prices")
    public CompletionStage<Void> consume(Message<String> price) {
        // process your price.

        // Acknowledge the incoming message, marking the RabbitMQ message as `accepted`.
        return price.ack();
    }

}
