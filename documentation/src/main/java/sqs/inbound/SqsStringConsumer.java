package sqs.inbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class SqsStringConsumer {
    @Incoming("data")
    void consume(String messageBody) {
        System.out.println("Received: " + messageBody);
    }
}
