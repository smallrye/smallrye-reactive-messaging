package io.smallrye.reactive.messaging.rabbitmq.converter;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.vertx.core.buffer.Buffer;

@ApplicationScoped
public class StringMessageConverter implements MessageConverter {

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return in instanceof IncomingRabbitMQMessage<?>
                && ((IncomingRabbitMQMessage<?>) in).getContentEncoding().isEmpty()
                && ((IncomingRabbitMQMessage<?>) in).getEffectiveContentType()
                        .map(contentType -> HttpHeaderValues.TEXT_PLAIN.toString().equalsIgnoreCase(contentType))
                        .orElse(false)
                && target == String.class;
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        return in.withPayload(Buffer.buffer((byte[]) in.getPayload()).toString());
    }
}
