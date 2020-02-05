package acme;

import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;

import io.smallrye.reactive.messaging.amqp.IncomingAmqpMetadata;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.amqp.AmqpMessageHelper;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class Receiver {

    @Incoming("my-topic")
    public CompletionStage<Void> consume(Message<String> message) {
        System.out.println("received (my-topic): " + message.getPayload() + " from address " +
            message.getMetadata(IncomingAmqpMetadata.class).get().getAddress());
        return message.ack();
    }

}
