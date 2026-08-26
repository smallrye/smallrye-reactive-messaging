package io.smallrye.reactive.messaging.mqtt;

import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * Used to represent MQTT metadata in on outgoing message.
 */
public final class SendingMqttMessageMetadata implements MqttMessageMetadata {

    private final String topic;
    private final MqttQoS qos;
    private final boolean isRetain;
    private final MqttProperties properties;

    public SendingMqttMessageMetadata(String topic, MqttQoS qos, boolean isRetain) {
        this(topic, qos, isRetain, MqttProperties.NO_PROPERTIES);
    }

    public SendingMqttMessageMetadata(String topic, MqttQoS qos, boolean isRetain, MqttProperties properties) {
        this.topic = topic;
        this.qos = qos;
        this.isRetain = isRetain;
        this.properties = properties != null ? properties : MqttProperties.NO_PROPERTIES;
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

    public MqttProperties getProperties() {
        return properties;
    }
}
