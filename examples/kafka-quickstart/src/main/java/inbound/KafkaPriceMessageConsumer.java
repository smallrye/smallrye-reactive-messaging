package inbound;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class KafkaPriceMessageConsumer {

    @Incoming("prices")
    public CompletionStage<Void> consume(Message<Double> price) {
        // process your price.
        System.out.println(
                "KafkaPriceMessageConsumer: consume " + price.getPayload());

        // Acknowledge the incoming message (commit the offset)
        return price.ack();
    }

}
