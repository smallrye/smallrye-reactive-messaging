package amqp.outbound;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.amqp.OutgoingAmqpMetadata;

public class AmqpOutboundDynamicAddressExample {
    public Message<Double> metadata(Message<Double> incoming) {

        // <code>
        String addressName = selectAddressFromIncommingMessage(incoming);
        OutgoingAmqpMetadata metadata = OutgoingAmqpMetadata.builder()
                .withAddress(addressName)
                .withDurable(true)
                .build();

        // Create a new message from the `incoming` message
        // Add `metadata` to the metadata from the `incoming` message.
        return incoming.addMetadata(metadata);
        // </code>
    }

    private String selectAddressFromIncommingMessage(Message<Double> incoming) {
        return "fake";
    }
}
