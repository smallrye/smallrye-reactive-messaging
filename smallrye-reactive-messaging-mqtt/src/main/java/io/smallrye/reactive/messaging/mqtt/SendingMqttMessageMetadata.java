package io.smallrye.reactive.messaging.mqtt;

import java.util.Collections;
import java.util.Map;

import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * Used to represent MQTT metadata in an outgoing message.
 */
public final class SendingMqttMessageMetadata implements MqttMessageMetadata {

    private final String topic;
    private final MqttQoS qos;
    private final boolean isRetain;

    // MQTT v5 properties
    private final Integer messageExpiryInterval;
    private final String contentType;
    private final String responseTopic;
    private final byte[] correlationData;
    private final Map<String, String> userProperties;
    private final Integer payloadFormatIndicator;

    /**
     * Create metadata with basic MQTT 3.1.1 fields.
     */
    public SendingMqttMessageMetadata(String topic, MqttQoS qos, boolean isRetain) {
        this(topic, qos, isRetain, null, null, null, null, null, null);
    }

    /**
     * Create metadata with MQTT v5 properties.
     */
    SendingMqttMessageMetadata(String topic, MqttQoS qos, boolean isRetain,
            Integer messageExpiryInterval, String contentType, String responseTopic,
            byte[] correlationData, Map<String, String> userProperties,
            Integer payloadFormatIndicator) {
        this.topic = topic;
        this.qos = qos;
        this.isRetain = isRetain;
        this.messageExpiryInterval = messageExpiryInterval;
        this.contentType = contentType;
        this.responseTopic = responseTopic;
        this.correlationData = correlationData;
        this.userProperties = userProperties != null ? Collections.unmodifiableMap(userProperties) : null;
        this.payloadFormatIndicator = payloadFormatIndicator;
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

    /**
     * @return the message expiry interval in seconds (MQTT 5.0), or {@code null} if not set
     */
    public Integer getMessageExpiryInterval() {
        return messageExpiryInterval;
    }

    /**
     * @return the content type of the payload (MQTT 5.0), or {@code null} if not set
     */
    public String getContentType() {
        return contentType;
    }

    /**
     * @return the response topic for request/response (MQTT 5.0), or {@code null} if not set
     */
    public String getResponseTopic() {
        return responseTopic;
    }

    /**
     * @return the correlation data for request/response (MQTT 5.0), or {@code null} if not set
     */
    public byte[] getCorrelationData() {
        return correlationData;
    }

    /**
     * @return the user properties (MQTT 5.0), or {@code null} if not set
     */
    public Map<String, String> getUserProperties() {
        return userProperties;
    }

    /**
     * @return the payload format indicator (MQTT 5.0): 0=unspecified, 1=UTF-8, or {@code null} if not set
     */
    public Integer getPayloadFormatIndicator() {
        return payloadFormatIndicator;
    }

    /**
     * @return {@code true} if any MQTT v5 property is set
     */
    public boolean hasV5Properties() {
        return messageExpiryInterval != null || contentType != null || responseTopic != null
                || correlationData != null || (userProperties != null && !userProperties.isEmpty())
                || payloadFormatIndicator != null;
    }
}
