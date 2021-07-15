package inbound;

import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata;

import org.eclipse.microprofile.reactive.messaging.Message;

import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;

public class RabbitMQMetadataExample {

    public void metadata(final Message<String> incomingMessage) {
        // tag::code[]
        final Optional<IncomingRabbitMQMetadata> metadata = incomingMessage.getMetadata(IncomingRabbitMQMetadata.class);
        metadata.ifPresent(meta -> {
            final Optional<String> contentEncoding = meta.getContentEncoding();
            final Optional<String> contentType = meta.getContentType();
            final Optional<String> correlationId = meta.getCorrelationId();
            final Optional<ZonedDateTime> creationTime = meta.getCreationTime(ZoneId.systemDefault());
            final Optional<Integer> priority = meta.getPriority();
            final Optional<String> replyTo = meta.getReplyTo();
            final Optional<String> userId = meta.getUserId();

            // Access a single String-valued header
            final Optional<String> stringHeader = meta.getHeader("my-header", String.class);

            // Access all headers
            final Map<String,Object> headers = meta.getHeaders();
            // ...
        });
        // end::code[]
    }

}
