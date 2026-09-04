package io.smallrye.reactive.messaging.mqtt;

import java.util.LinkedHashMap;
import java.util.Map;

import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * Builder for {@link SendingMqttMessageMetadata} with support for MQTT v5 properties.
 */
public class SendingMqttMessageMetadataBuilder {

    private String topic;
    private MqttQoS qos;
    private boolean retain;
    private Integer messageExpiryInterval;
    private String contentType;
    private String responseTopic;
    private byte[] correlationData;
    private Map<String, String> userProperties;
    private Integer payloadFormatIndicator;

    SendingMqttMessageMetadataBuilder() {
    }

    /**
     * Create a new builder instance.
     */
    public static SendingMqttMessageMetadataBuilder builder() {
        return new SendingMqttMessageMetadataBuilder();
    }

    /**
     * Set the topic to publish to.
     */
    public SendingMqttMessageMetadataBuilder withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    /**
     * Set the QoS level.
     */
    public SendingMqttMessageMetadataBuilder withQos(MqttQoS qos) {
        this.qos = qos;
        return this;
    }

    /**
     * Set whether the message should be retained.
     */
    public SendingMqttMessageMetadataBuilder withRetain(boolean retain) {
        this.retain = retain;
        return this;
    }

    /**
     * Set the message expiry interval in seconds (MQTT 5.0).
     */
    public SendingMqttMessageMetadataBuilder withMessageExpiryInterval(int messageExpiryInterval) {
        this.messageExpiryInterval = messageExpiryInterval;
        return this;
    }

    /**
     * Set the content type of the payload (MQTT 5.0).
     */
    public SendingMqttMessageMetadataBuilder withContentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    /**
     * Set the response topic for request/response (MQTT 5.0).
     */
    public SendingMqttMessageMetadataBuilder withResponseTopic(String responseTopic) {
        this.responseTopic = responseTopic;
        return this;
    }

    /**
     * Set the correlation data for request/response (MQTT 5.0).
     */
    public SendingMqttMessageMetadataBuilder withCorrelationData(byte[] correlationData) {
        this.correlationData = correlationData;
        return this;
    }

    /**
     * Add a single user property (MQTT 5.0).
     */
    public SendingMqttMessageMetadataBuilder withUserProperty(String key, String value) {
        if (this.userProperties == null) {
            this.userProperties = new LinkedHashMap<>();
        }
        this.userProperties.put(key, value);
        return this;
    }

    /**
     * Set all user properties (MQTT 5.0). Replaces any previously set user properties.
     */
    public SendingMqttMessageMetadataBuilder withUserProperties(Map<String, String> userProperties) {
        this.userProperties = userProperties != null ? new LinkedHashMap<>(userProperties) : null;
        return this;
    }

    /**
     * Set the payload format indicator (MQTT 5.0): 0=unspecified bytes, 1=UTF-8 encoded.
     */
    public SendingMqttMessageMetadataBuilder withPayloadFormatIndicator(int payloadFormatIndicator) {
        this.payloadFormatIndicator = payloadFormatIndicator;
        return this;
    }

    /**
     * Build the metadata instance.
     */
    public SendingMqttMessageMetadata build() {
        return new SendingMqttMessageMetadata(topic, qos, retain,
                messageExpiryInterval, contentType, responseTopic,
                correlationData, userProperties, payloadFormatIndicator);
    }
}
