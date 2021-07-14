package inbound;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class RabbitMQPriceMessageConsumer {

    @Incoming("prices")
    public CompletionStage<Void> consume(Message<String> price) {
        // process your price.

        // Acknowledge the incoming message, marking the RabbitMQ message as `accepted`.
        return price.ack();
    }

}
