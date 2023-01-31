package mqtt.inbound;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class MqttPriceMessageConsumer {

    @Incoming("prices")
    public CompletionStage<Void> consume(Message<byte[]> price) {
        // process your price.

        // Acknowledge the incoming message
        return price.ack();
    }

}
