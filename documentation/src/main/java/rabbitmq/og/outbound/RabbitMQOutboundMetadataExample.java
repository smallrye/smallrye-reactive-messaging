package rabbitmq.og.outbound;

import java.util.Date;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.rabbitmq.og.OutgoingRabbitMQMetadata;

public class RabbitMQOutboundMetadataExample {

    public Message<String> metadata(Message<String> incoming) {

        // <code>
        final OutgoingRabbitMQMetadata metadata = OutgoingRabbitMQMetadata.builder()
                .withHeader("my-header", "xyzzy")
                .withRoutingKey("urgent")
                .withTimestamp(new Date())
                .build();

        // Add `metadata` to the metadata of the outgoing message.
        return Message.of("Hello", Metadata.of(metadata));
        // </code>
    }

}
