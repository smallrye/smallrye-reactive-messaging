package acme;

import io.smallrye.reactive.messaging.pulsar.PulsarMessage;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class Receiver {

    @Incoming("sink")
    public CompletionStage<Void> consume(PulsarMessage message) {
        byte[] payload = message.getPayload();
        String key = message.getKey();
        System.out.println("received: " + payload + " from topic " + message.getTopicName());
        return message.ack();
    }

}
