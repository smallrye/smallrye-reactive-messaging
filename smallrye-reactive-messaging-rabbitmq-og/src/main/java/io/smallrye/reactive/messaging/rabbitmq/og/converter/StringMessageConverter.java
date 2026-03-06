package io.smallrye.reactive.messaging.rabbitmq.og.converter;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMetadata;

/**
 * Fallback converter that converts any RabbitMQ {@code byte[]} payload to {@code String}.
 */
@ApplicationScoped
public class StringMessageConverter implements MessageConverter {

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return String.class.equals(target)
                && in.getMetadata(IncomingRabbitMQMetadata.class).isPresent();
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        IncomingRabbitMQMetadata metadata = in.getMetadata(IncomingRabbitMQMetadata.class)
                .orElseThrow(() -> new IllegalStateException("No RabbitMQ metadata"));
        byte[] body = metadata.getBody();
        return in.withPayload(new String(body, StandardCharsets.UTF_8));
    }

    @Override
    public int getPriority() {
        return Integer.MAX_VALUE - 1;
    }
}
