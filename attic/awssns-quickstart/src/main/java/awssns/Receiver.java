package awssns;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class Receiver {

    @Incoming("my-topic")
    public CompletionStage<Void> consume(Message<String> message) {
        System.out.println("received (my-topic): " + message.getPayload());
        return message.ack();
    }

}
