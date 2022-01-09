package amqp.outbound;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.amqp.OutgoingAmqpMetadata;

public class AmqpOutboundMetadataExample {

    public Message<Double> metadata(Message<Double> incoming) {

        // <code>
        OutgoingAmqpMetadata metadata = OutgoingAmqpMetadata.builder()
                .withDurable(true)
                .withSubject("my-subject")
                .build();

        // Create a new message from the `incoming` message
        // Add `metadata` to the metadata from the `incoming` message.
        return incoming.addMetadata(metadata);
        // </code>
    }

}
