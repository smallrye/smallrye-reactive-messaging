package io.smallrye.reactive.messaging.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.mutiny.mqtt.messages.MqttPublishMessage;

/**
 * Used to represent MQTT metadata of an incoming message.
 */
public class ReceivingMqttMessageMetadata implements MqttMessageMetadata {

    private final MqttPublishMessage message;

    public ReceivingMqttMessageMetadata(MqttPublishMessage message) {
        this.message = message;
    }

    /**
     * @return the message id of the MQTT message
     */
    public int getMessageId() {
        return message.messageId();
    }

    @Override
    public String getTopic() {
        return message.topicName();
    }

    @Override
    public MqttQoS getQosLevel() {
        return message.qosLevel();
    }

    @Override
    public boolean isRetain() {
        return message.isRetain();
    }

    /**
     * @return {@code true} if the message is a duplicate
     */
    public boolean isDuplicate() {
        return message.isDup();
    }

}
