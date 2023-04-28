package io.smallrye.reactive.messaging.mqtt.converter;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.mqtt.ReceivingMqttMessageMetadata;
import io.vertx.core.buffer.Buffer;

@ApplicationScoped
public class StringMessageConverter implements MessageConverter {
    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return in.getMetadata(ReceivingMqttMessageMetadata.class).isPresent()
                && target == String.class;
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        return in.withPayload(Buffer.buffer((byte[]) in.getPayload()).toString());
    }
}
