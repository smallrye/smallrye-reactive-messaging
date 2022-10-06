package gcp.pubsub;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.gcp.pubsub.PubSubMessage;

@ApplicationScoped
public class Receiver {

    @Incoming("my-topic")
    public CompletionStage<Void> consume(final PubSubMessage message) {
        System.out.println("received (my-topic): " + message.getPayload());
        return message.ack();
    }

}
