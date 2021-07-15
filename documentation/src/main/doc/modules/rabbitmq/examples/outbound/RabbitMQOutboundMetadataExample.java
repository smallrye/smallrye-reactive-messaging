package outbound;

import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import java.time.ZonedDateTime;

public class RabbitMQOutboundMetadataExample {

    public Message<String> metadata(Message<String> incoming) {

        // tag::code[]
        final OutgoingRabbitMQMetadata metadata = new OutgoingRabbitMQMetadata.Builder()
                .withHeader("my-header", "xyzzy")
                .withRoutingKey("urgent")
                .withTimestamp(ZonedDateTime.now())
                .build();

        // Add `metadata` to the metadata of the outgoing message.
        return Message.of("Hello", Metadata.of(metadata));
        // end::code[]
    }

}
