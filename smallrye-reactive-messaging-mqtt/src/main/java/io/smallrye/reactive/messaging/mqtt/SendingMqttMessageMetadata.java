package io.smallrye.reactive.messaging.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * Used to represent MQTT metadata in on outgoing message.
 */
public final class SendingMqttMessageMetadata implements MqttMessageMetadata {

    private final String topic;
    private final MqttQoS qos;
    private final boolean isRetain;

    public SendingMqttMessageMetadata(String topic, MqttQoS qos, boolean isRetain) {
        this.topic = topic;
        this.qos = qos;
        this.isRetain = isRetain;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public MqttQoS getQosLevel() {
        return qos;
    }

    @Override
    public boolean isRetain() {
        return isRetain;
    }
}
