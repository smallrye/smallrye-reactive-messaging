package io.smallrye.reactive.messaging.amqp.converters;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.amqp.IncomingAmqpMetadata;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;

@ApplicationScoped
public class AmqpMessageConverter implements MessageConverter {
    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return in.getMetadata(IncomingAmqpMetadata.class).isPresent()
                && TypeUtils.isAssignable(target, io.vertx.amqp.AmqpMessage.class);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        IncomingAmqpMetadata metadata = in.getMetadata(IncomingAmqpMetadata.class)
                .orElseThrow(() -> new IllegalStateException("No AMQP 1.0 metadata"));
        return in.withPayload(metadata.getMessage());
    }
}
