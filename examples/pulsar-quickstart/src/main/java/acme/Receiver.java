package acme;

import java.time.Instant;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;

import io.smallrye.reactive.messaging.pulsar.PulsarMessage;
import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class Receiver {

    @Incoming("pulsar")
    public CompletionStage<Void> consume(PulsarMessage message) {
        byte[] payload = message.getPayload();
        String key = message.getKey();
        System.out.println("received: " + payload + " from topic " + message.getTopicName());
        return message.ack();
    }

}
