package io.smallrye.reactive.messaging.rabbitmq;

import java.lang.reflect.Type;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;

/**
 * Fails converting any message that has the "should-fail-conversion" header with
 * the value "true". Otherwise it converts string bodies into integer bodies.
 */
@ApplicationScoped
public class FailingMessageConverter implements MessageConverter {

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return target.equals(Integer.class);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        IncomingRabbitMQMetadata metaData = in.getMetadata(IncomingRabbitMQMetadata.class).orElse(null);
        if (metaData != null && metaData.getHeader("should-fail-conversion", String.class).orElse("").equals("true")) {
            throw new RuntimeException("Message Failed Conversion");
        }
        return in.withPayload(Integer.parseInt(in.getPayload().toString()));
    }
}
