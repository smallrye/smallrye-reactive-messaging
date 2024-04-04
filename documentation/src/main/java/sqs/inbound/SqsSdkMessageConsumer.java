package sqs.inbound;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

@ApplicationScoped
public class SqsSdkMessageConsumer {

    @Incoming("data")
    void consume(Message msg) {
        System.out.println("Received: " + msg.body());
        Map<String, MessageAttributeValue> attributes = msg.messageAttributes();
        // ...
    }
}
