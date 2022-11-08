package io.smallrye.reactive.messaging.mqtt;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public interface MqttMessage<T> extends ContextAwareMessage<T> {

    static <T> MqttMessage<T> of(T payload) {
        return new SendingMqttMessage<>(payload, new SendingMqttMessageMetadata(null, null, false), null);
    }

    static <T> MqttMessage<T> of(SendingMqttMessageMetadata metadata, T payload) {
        return new SendingMqttMessage<>(payload, metadata, null);
    }

    static <T> MqttMessage<T> of(SendingMqttMessageMetadata metadata, T payload, Supplier<CompletionStage<Void>> ack) {
        return new SendingMqttMessage<>(payload, metadata, ack);
    }

    static <T> MqttMessage<T> of(String topic, T payload) {
        return new SendingMqttMessage<>(payload, new SendingMqttMessageMetadata(topic, null, false), null);
    }

    static <T> MqttMessage<T> of(String topic, T payload, Supplier<CompletionStage<Void>> ack) {
        return new SendingMqttMessage<>(payload, new SendingMqttMessageMetadata(topic, null, false), ack);
    }

    static <T> MqttMessage<T> of(String topic, T payload, MqttQoS qos) {
        return new SendingMqttMessage<>(payload, new SendingMqttMessageMetadata(topic, qos, false));
    }

    static <T> MqttMessage<T> of(String topic, T payload, MqttQoS qos, boolean retain) {
        return new SendingMqttMessage<>(payload, new SendingMqttMessageMetadata(topic, qos, retain));
    }

    default MqttMessage<T> withAck(Supplier<CompletionStage<Void>> ack) {
        return new SendingMqttMessage<>(getPayload(), new SendingMqttMessageMetadata(getTopic(), getQosLevel(), isRetain()),
                ack);
    }

    int getMessageId();

    MqttQoS getQosLevel();

    boolean isDuplicate();

    boolean isRetain();

    String getTopic();
}
