package inbound;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class EventBusPriceMessageConsumer {

    @Incoming("prices")
    public CompletionStage<Void> consume(Message<Double> price) {
        // process your price.

        // TASK - Explain acknowledgement
        // Acknowledge the incoming message
        return price.ack();
    }

}
