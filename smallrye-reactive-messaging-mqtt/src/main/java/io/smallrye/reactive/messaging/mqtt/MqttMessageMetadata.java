package io.smallrye.reactive.messaging.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;

public interface MqttMessageMetadata {

    /**
     * @return topic of the MQTT message
     */
    String getTopic();

    /**
     * @return the qos level of the MQTT message
     */
    MqttQoS getQosLevel();

    /**
     * @return {@code true} if the MQTT message is retained
     */
    boolean isRetain();

}
