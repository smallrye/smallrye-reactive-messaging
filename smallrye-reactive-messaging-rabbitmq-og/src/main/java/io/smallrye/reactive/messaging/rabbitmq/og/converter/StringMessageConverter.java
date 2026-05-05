package io.smallrye.reactive.messaging.rabbitmq.og.converter;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMetadata;

@ApplicationScoped
public class StringMessageConverter implements MessageConverter {

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        if (!String.class.equals(target)) {
            return false;
        }
        Optional<IncomingRabbitMQMetadata> maybe = in.getMetadata(IncomingRabbitMQMetadata.class);
        if (maybe.isEmpty()) {
            return false;
        }
        IncomingRabbitMQMetadata metadata = maybe.get();
        String encoding = metadata.getContentEncoding();
        return (encoding == null || encoding.isEmpty())
                && metadata.getEffectiveContentType()
                        .map(contentType -> "text/plain".equalsIgnoreCase(contentType))
                        .orElse(false);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        IncomingRabbitMQMetadata metadata = in.getMetadata(IncomingRabbitMQMetadata.class)
                .orElseThrow(() -> new IllegalStateException("No RabbitMQ metadata"));
        byte[] body = metadata.getBody();
        return in.withPayload(new String(body, StandardCharsets.UTF_8));
    }
}
