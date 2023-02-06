package acme;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.amqp.AmqpMessage;

@ApplicationScoped
public class Receiver {

    @Incoming("my-topic")
    public CompletionStage<Void> consume(AmqpMessage<String> message) {
        System.out.println("received (my-topic): " + message.getPayload() + " from address " + message.getAddress());
        return message.ack();
    }

}
