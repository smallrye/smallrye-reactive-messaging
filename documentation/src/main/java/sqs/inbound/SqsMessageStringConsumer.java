package sqs.inbound;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class SqsMessageStringConsumer {
    @Incoming("data")
    CompletionStage<Void> consume(Message<String> msg) {
        System.out.println("Received: " + msg.getPayload());
        return msg.ack();
    }
}
