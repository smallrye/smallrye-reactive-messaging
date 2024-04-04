package sqs.inbound;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.aws.sqs.SqsIncomingMetadata;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

@ApplicationScoped
public class SqsMetadataExample {

    @Incoming("queue")
    public void metadata(String body, SqsIncomingMetadata metadata) {
        Map<String, MessageAttributeValue> attributes = metadata.getMessage().messageAttributes();
        attributes.forEach((k, v) -> System.out.println(k + " -> " + v.stringValue()));
        System.out.println("Message body: " + body);
    }
}
