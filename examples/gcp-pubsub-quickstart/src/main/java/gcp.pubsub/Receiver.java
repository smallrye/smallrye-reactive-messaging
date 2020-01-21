package gcp.pubsub;

import io.smallrye.reactive.messaging.gcp.pubsub.PubSubMessage;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class Receiver {

    @Incoming("my-topic")
    public CompletionStage<Void> consume(final PubSubMessage message) {
        System.out.println("received (my-topic): " + message.getPayload());
        return message.ack();
    }

}
