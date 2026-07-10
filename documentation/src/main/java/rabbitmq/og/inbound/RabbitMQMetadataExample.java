package rabbitmq.og.inbound;

import java.util.Date;
import java.util.Map;
import java.util.Optional;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMetadata;

public class RabbitMQMetadataExample {

    public void metadata(final Message<String> incomingMessage) {
        // <code>
        final Optional<IncomingRabbitMQMetadata> metadata = incomingMessage.getMetadata(IncomingRabbitMQMetadata.class);
        metadata.ifPresent(meta -> {
            final String contentEncoding = meta.getContentEncoding();
            final String contentType = meta.getContentType();
            final String correlationId = meta.getCorrelationId();
            final Date timestamp = meta.getTimestamp();
            final Integer priority = meta.getPriority();
            final String replyTo = meta.getReplyTo();
            final String userId = meta.getUserId();

            // Access a single String-valued header
            final Optional<String> stringHeader = meta.getHeader("my-header");

            // Access all headers
            final Map<String, Object> headers = meta.getHeaders();
            // ...
        });
        // </code>
    }

}
