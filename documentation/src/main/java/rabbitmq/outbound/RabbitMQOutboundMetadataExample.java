package rabbitmq.outbound;

import java.time.ZonedDateTime;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;

public class RabbitMQOutboundMetadataExample {

    public Message<String> metadata(Message<String> incoming) {

        // <code>
        final OutgoingRabbitMQMetadata metadata = new OutgoingRabbitMQMetadata.Builder()
                .withHeader("my-header", "xyzzy")
                .withRoutingKey("urgent")
                .withTimestamp(ZonedDateTime.now())
                .build();

        // Add `metadata` to the metadata of the outgoing message.
        return Message.of("Hello", Metadata.of(metadata));
        // </code>
    }

}
