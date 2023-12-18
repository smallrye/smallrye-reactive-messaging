package io.smallrye.reactive.messaging.mqtt.converter;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.mqtt.ReceivingMqttMessageMetadata;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;
import io.vertx.mqtt.messages.MqttPublishMessage;

@ApplicationScoped
public class MqttMessageConverter implements MessageConverter {
    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return in.getMetadata(ReceivingMqttMessageMetadata.class).isPresent()
                && TypeUtils.isAssignable(target, MqttPublishMessage.class);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        ReceivingMqttMessageMetadata metadata = in.getMetadata(ReceivingMqttMessageMetadata.class)
                .orElseThrow(() -> new IllegalStateException("No MQTT metadata"));
        return in.withPayload(metadata.getMessage().getDelegate());
    }
}
