package sns.outbound;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.aws.sns.SnsOutboundMetadata;

import software.amazon.awssdk.services.sns.model.MessageAttributeValue;

@ApplicationScoped
public class SnsOutboundMetadataExample {

    public Message<String> metadata(Message<String> incoming) {
        // <code>
        final SnsOutboundMetadata metadata = SnsOutboundMetadata.builder()
                .messageAttributes(Map.of("my-attribute", MessageAttributeValue.builder()
                        .dataType("String").stringValue("my-value").build()))
                .groupId("group-1")
                .smsPhoneNumber("+1234567890")
                .build();

        // Add `metadata` to the metadata of the outgoing message.
        return Message.of("Hello", Metadata.of(metadata));
        // </code>
    }
}
