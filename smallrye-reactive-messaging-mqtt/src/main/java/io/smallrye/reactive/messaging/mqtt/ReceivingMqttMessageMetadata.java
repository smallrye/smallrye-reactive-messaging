package io.smallrye.reactive.messaging.mqtt;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    // -------------------------------------------------------------------------
    // MQTT 5.0 property accessors
    // -------------------------------------------------------------------------

    /**
     * @return the raw MQTT properties, or {@code null} if the underlying message has no properties
     */
    public MqttProperties getProperties() {
        return message.getDelegate().properties();
    }

    /**
     * @return the message expiry interval in seconds (MQTT 5.0), or {@code null} if not present
     */
    public Integer getMessageExpiryInterval() {
        return getIntegerProperty(MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL);
    }

    /**
     * @return the content type of the payload (MQTT 5.0), or {@code null} if not present
     */
    public String getContentType() {
        return getStringProperty(MqttProperties.MqttPropertyType.CONTENT_TYPE);
    }

    /**
     * @return the response topic for request/response (MQTT 5.0), or {@code null} if not present
     */
    public String getResponseTopic() {
        return getStringProperty(MqttProperties.MqttPropertyType.RESPONSE_TOPIC);
    }

    /**
     * @return the correlation data for request/response (MQTT 5.0), or {@code null} if not present
     */
    public byte[] getCorrelationData() {
        MqttProperties properties = getProperties();
        if (properties == null) {
            return null;
        }
        MqttProperties.MqttProperty<?> prop = properties
                .getProperty(MqttProperties.MqttPropertyType.CORRELATION_DATA.value());
        if (prop instanceof MqttProperties.BinaryProperty) {
            return ((MqttProperties.BinaryProperty) prop).value();
        }
        return null;
    }

    /**
     * @return the user properties as a map (MQTT 5.0), or {@code null} if not present.
     *         Note: duplicate keys are collapsed to the last value.
     */
    public Map<String, String> getUserProperties() {
        MqttProperties properties = getProperties();
        if (properties == null) {
            return null;
        }
        List<MqttProperties.UserProperty> userProps = (List<MqttProperties.UserProperty>) (List<?>) properties
                .getProperties(MqttProperties.MqttPropertyType.USER_PROPERTY.value());
        if (userProps == null || userProps.isEmpty()) {
            return null;
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (MqttProperties.UserProperty up : userProps) {
            result.put(up.value().key, up.value().value);
        }
        return result;
    }

    /**
     * @return the payload format indicator (MQTT 5.0): 0=unspecified, 1=UTF-8, or {@code null} if not present
     */
    public Integer getPayloadFormatIndicator() {
        return getIntegerProperty(MqttProperties.MqttPropertyType.PAYLOAD_FORMAT_INDICATOR);
    }

    /**
     * @return the subscription identifier (MQTT 5.0), or {@code null} if not present
     */
    public Integer getSubscriptionIdentifier() {
        return getIntegerProperty(MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER);
    }

    private Integer getIntegerProperty(MqttProperties.MqttPropertyType type) {
        MqttProperties properties = getProperties();
        if (properties == null) {
            return null;
        }
        MqttProperties.MqttProperty<?> prop = properties.getProperty(type.value());
        if (prop instanceof MqttProperties.IntegerProperty) {
            return ((MqttProperties.IntegerProperty) prop).value();
        }
        return null;
    }

    private String getStringProperty(MqttProperties.MqttPropertyType type) {
        MqttProperties properties = getProperties();
        if (properties == null) {
            return null;
        }
        MqttProperties.MqttProperty<?> prop = properties.getProperty(type.value());
        if (prop instanceof MqttProperties.StringProperty) {
            return ((MqttProperties.StringProperty) prop).value();
        }
        return null;
    }
}
