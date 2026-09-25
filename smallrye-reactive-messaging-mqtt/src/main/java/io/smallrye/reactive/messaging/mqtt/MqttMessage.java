package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttExceptions.ex;

import java.util.Map;
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

    /**
     * Create the response to an MQTT 5.0 request: the message is sent on the `Response Topic` carried by the request,
     * with its `Correlation Data`, so that the requester can match the response with its request.
     *
     * @param request the incoming message to respond to, it must carry a `Response Topic`
     * @param payload the payload of the response
     * @param qos the QoS level of the response
     * @return the response message
     */
    static <T> MqttMessage<T> ofResponse(MqttMessage<?> request, T payload, MqttQoS qos) {
        return ofResponse(request, payload, qos, null);
    }

    /**
     * Create the response to an MQTT 5.0 request, with user properties.
     *
     * @param request the incoming message to respond to, it must carry a `Response Topic`
     * @param payload the payload of the response
     * @param qos the QoS level of the response
     * @param userProperties the MQTT 5.0 user properties of the response, can be {@code null}
     * @return the response message
     */
    static <T> MqttMessage<T> ofResponse(MqttMessage<?> request, T payload, MqttQoS qos,
            Map<String, String> userProperties) {
        String responseTopic = request.getResponseTopic();
        if (responseTopic == null) {
            throw ex.illegalArgumentMissingResponseTopic();
        }
        SendingMqttMessageMetadata metadata = SendingMqttMessageMetadataBuilder.builder()
                .withTopic(responseTopic)
                .withQos(qos)
                .withCorrelationData(request.getCorrelationData())
                .withUserProperties(userProperties)
                .build();
        return new SendingMqttMessage<>(payload, metadata);
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

    /**
     * @return the `Response Topic` of an incoming MQTT 5.0 message, or {@code null} if it has none
     */
    default String getResponseTopic() {
        return getMetadata(ReceivingMqttMessageMetadata.class)
                .map(ReceivingMqttMessageMetadata::getResponseTopic)
                .orElse(null);
    }

    /**
     * @return the `Correlation Data` of an incoming MQTT 5.0 message, or {@code null} if it has none
     */
    default byte[] getCorrelationData() {
        return getMetadata(ReceivingMqttMessageMetadata.class)
                .map(ReceivingMqttMessageMetadata::getCorrelationData)
                .orElse(null);
    }
}
