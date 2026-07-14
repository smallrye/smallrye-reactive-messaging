package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttExceptions.ex;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import io.netty.handler.codec.mqtt.MqttProperties;
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

    static <T> MqttMessage<T> of(String topic, T payload, MqttQoS qos, boolean retain, MqttProperties properties) {
        return new SendingMqttMessage<>(payload, new SendingMqttMessageMetadata(topic, qos, retain, properties));
    }

    static <T> MqttMessage<T> of(String topic, T payload, MqttProperties properties) {
        return new SendingMqttMessage<>(payload, new SendingMqttMessageMetadata(topic, null, false, properties));
    }

    static <T> MqttMessage<T> ofResponse(MqttMessage<?> inputMessage, T payload, MqttQoS qos) {
        return ofResponse(inputMessage, payload, qos, new MqttProperties());
    }

    static <T> MqttMessage<T> ofResponse(MqttMessage<?> inputMessage, T payload, MqttQoS qos, MqttProperties properties) {
        String responseTopic = inputMessage.getResponseTopic();
        if (responseTopic == null) {
            throw ex.illegalArgumentMissingResponseTopic();
        }
        MqttProperties props = new MqttProperties();
        properties.listAll().forEach(props::add);
        byte[] correlationData = inputMessage.getCorrelationData();
        if (correlationData != null) {
            props.add(new MqttProperties.BinaryProperty(
                    MqttProperties.MqttPropertyType.CORRELATION_DATA.value(), correlationData));
        }
        return new SendingMqttMessage<>(payload, new SendingMqttMessageMetadata(responseTopic, qos, false, props));
    }

    // TODO Should be removed?
    default MqttMessage<T> withAck(Supplier<CompletionStage<Void>> ack) {
        MqttProperties props = getMetadata(SendingMqttMessageMetadata.class)
                .map(SendingMqttMessageMetadata::getProperties)
                .orElse(MqttProperties.NO_PROPERTIES);
        return new SendingMqttMessage<>(getPayload(),
                new SendingMqttMessageMetadata(getTopic(), getQosLevel(), isRetain(), props), ack);
    }

    int getMessageId();

    MqttQoS getQosLevel();

    boolean isDuplicate();

    boolean isRetain();

    String getTopic();

    default String getResponseTopic() {
        return getMetadata(ReceivingMqttMessageMetadata.class)
                .map(ReceivingMqttMessageMetadata::getResponseTopic)
                .orElse(null);
    }

    default byte[] getCorrelationData() {
        return getMetadata(ReceivingMqttMessageMetadata.class)
                .map(ReceivingMqttMessageMetadata::getCorrelationData)
                .orElse(null);
    }
}
