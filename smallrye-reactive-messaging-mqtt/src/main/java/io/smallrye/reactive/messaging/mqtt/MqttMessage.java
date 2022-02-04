package io.smallrye.reactive.messaging.mqtt;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public interface MqttMessage<T> extends ContextAwareMessage<T> {

    static <T> MqttMessage<T> of(T payload) {
        return new SendingMqttMessage<>(null, payload, null, false, null);
    }

    static <T> MqttMessage<T> of(String topic, T payload) {
        return new SendingMqttMessage<>(topic, payload, null, false, null);
    }

    static <T> MqttMessage<T> of(String topic, T payload, Supplier<CompletionStage<Void>> ack) {
        return new SendingMqttMessage<>(topic, payload, null, false, ack);
    }

    static <T> MqttMessage<T> of(String topic, T payload, MqttQoS qos) {
        return new SendingMqttMessage<>(topic, payload, qos, false);
    }

    static <T> MqttMessage<T> of(String topic, T payload, MqttQoS qos, boolean retain) {
        return new SendingMqttMessage<>(topic, payload, qos, retain);
    }

    default MqttMessage<T> withAck(Supplier<CompletionStage<Void>> ack) {
        return new SendingMqttMessage<>(getTopic(), getPayload(), getQosLevel(), isRetain(), ack);
    }

    int getMessageId();

    MqttQoS getQosLevel();

    boolean isDuplicate();

    boolean isRetain();

    String getTopic();
}
