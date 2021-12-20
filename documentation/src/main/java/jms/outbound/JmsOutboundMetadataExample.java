package jms.outbound;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.jms.JmsProperties;
import io.smallrye.reactive.messaging.jms.OutgoingJmsMessageMetadata;

public class JmsOutboundMetadataExample {

    public Message<Double> metadata(Message<Double> incoming) {

        // <code>
        OutgoingJmsMessageMetadata metadata = OutgoingJmsMessageMetadata.builder()
                .withProperties(JmsProperties.builder().with("some-property", "some-value").build())
                .build();

        // Create a new message from the `incoming` message
        // Add `metadata` to the metadata from the `incoming` message.
        return incoming.addMetadata(metadata);
        // </code>
    }

}
