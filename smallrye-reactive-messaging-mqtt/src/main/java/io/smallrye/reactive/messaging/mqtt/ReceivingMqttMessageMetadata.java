package io.smallrye.reactive.messaging.mqtt;

import java.util.Collections;
import java.util.List;

import io.netty.handler.codec.mqtt.MqttProperties;
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
     * @return the MQTT message
     */
    public MqttPublishMessage getMessage() {
        return message;
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

    /**
     * @return the MQTT 5.0 properties of the message, or {@code NO_PROPERTIES} for MQTT 3.1.1
     */
    public MqttProperties getProperties() {
        return message.properties();
    }

    /**
     * @return the User Properties from the MQTT 5.0 message, or empty list if not present
     */
    public List<MqttProperties.StringPair> getUserProperties() {
        MqttProperties.MqttProperty<?> prop = getProperties()
                .getProperty(MqttProperties.MqttPropertyType.USER_PROPERTY.value());
        if (prop instanceof MqttProperties.UserProperties) {
            return ((MqttProperties.UserProperties) prop).value();
        }
        return Collections.emptyList();
    }

    /**
     * @return the Content-Type property from the MQTT 5.0 message, or null if not present
     */
    public String getContentType() {
        MqttProperties.MqttProperty<?> prop = getProperties()
                .getProperty(MqttProperties.MqttPropertyType.CONTENT_TYPE.value());
        return prop != null ? (String) prop.value() : null;
    }

    /**
     * @return the Response Topic property from the MQTT 5.0 message, or null if not present
     */
    public String getResponseTopic() {
        MqttProperties.MqttProperty<?> prop = getProperties()
                .getProperty(MqttProperties.MqttPropertyType.RESPONSE_TOPIC.value());
        return prop != null ? (String) prop.value() : null;
    }

    /**
     * @return the Correlation Data property from the MQTT 5.0 message, or null if not present
     */
    public byte[] getCorrelationData() {
        MqttProperties.MqttProperty<?> prop = getProperties()
                .getProperty(MqttProperties.MqttPropertyType.CORRELATION_DATA.value());
        return prop != null ? (byte[]) prop.value() : null;
    }

    /**
     * @return the Message Expiry Interval from the MQTT 5.0 message (in seconds), or null if not present
     */
    public Integer getMessageExpiryInterval() {
        MqttProperties.MqttProperty<?> prop = getProperties()
                .getProperty(MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL.value());
        return prop != null ? (Integer) prop.value() : null;
    }

    /**
     * @return the Payload Format Indicator (0=binary, 1=UTF-8), or null if not present
     */
    public Integer getPayloadFormatIndicator() {
        MqttProperties.MqttProperty<?> prop = getProperties()
                .getProperty(MqttProperties.MqttPropertyType.PAYLOAD_FORMAT_INDICATOR.value());
        return prop != null ? (Integer) prop.value() : null;
    }

}
