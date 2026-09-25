package io.smallrye.reactive.messaging.rabbitmq.converter;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata;
import io.vertx.rabbitmq.RabbitMQMessage;

@ApplicationScoped
public class ByteArrayMessageConverter implements MessageConverter {

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return byte[].class.equals(target)
                && in.getMetadata(IncomingRabbitMQMetadata.class).isPresent();
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        IncomingRabbitMQMetadata metadata = in.getMetadata(IncomingRabbitMQMetadata.class)
                .orElseThrow(() -> new IllegalStateException("No RabbitMQ metadata"));
        RabbitMQMessage message = metadata.getMessage()
                .orElseThrow(() -> new IllegalStateException("No RabbitMQ message"));
        return in.withPayload(message.body().getBytes());
    }
}
