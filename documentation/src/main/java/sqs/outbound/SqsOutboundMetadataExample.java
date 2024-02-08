package sqs.outbound;

import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.aws.sqs.SqsOutboundMetadata;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

public class SqsOutboundMetadataExample {

    public Message<String> metadata(Message<String> incoming) {

        // <code>
        final SqsOutboundMetadata metadata = SqsOutboundMetadata.builder()
                .messageAttributes(Map.of("my-attribute", MessageAttributeValue.builder()
                        .dataType("String").stringValue("my-value").build()))
                .groupId("group-1")
                .build();

        // Add `metadata` to the metadata of the outgoing message.
        return Message.of("Hello", Metadata.of(metadata));
        // </code>
    }

}
