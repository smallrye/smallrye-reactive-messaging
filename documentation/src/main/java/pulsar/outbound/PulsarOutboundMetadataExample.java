package pulsar.outbound;

import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.pulsar.PulsarOutgoingMessageMetadata;

public class PulsarOutboundMetadataExample {

    public Message<Double> metadata(Message<Double> incoming) {
        // <code>
        // Creates an PulsarOutgoingMessageMetadata
        // The type parameter is the type of the record's key
        PulsarOutgoingMessageMetadata metadata = PulsarOutgoingMessageMetadata.builder()
                .withKey("my-key")
                .withProperties(Map.of("property-key", "value"))
                .build();

        // Create a new message from the `incoming` message
        // Add `metadata` to the metadata from the `incoming` message.
        return incoming.addMetadata(metadata);
        // </code>
    }

}
