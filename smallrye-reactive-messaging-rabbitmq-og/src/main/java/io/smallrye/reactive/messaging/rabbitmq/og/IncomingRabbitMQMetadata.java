package io.smallrye.reactive.messaging.rabbitmq.og;

import java.util.*;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

/**
 * Metadata for incoming RabbitMQ messages.
 * Provides access to envelope, properties, and message metadata.
 */
public class IncomingRabbitMQMetadata {

    private final Envelope envelope;
    private final AMQP.BasicProperties properties;
    private final byte[] body;
    private final String contentTypeOverride;

    public IncomingRabbitMQMetadata(Envelope envelope, AMQP.BasicProperties properties) {
        this(envelope, properties, null, null);
    }

    public IncomingRabbitMQMetadata(Envelope envelope, AMQP.BasicProperties properties,
            byte[] body, String contentTypeOverride) {
        this.envelope = envelope;
        this.properties = properties;
        this.body = body;
        this.contentTypeOverride = contentTypeOverride;
    }

    public Envelope getEnvelope() {
        return envelope;
    }

    public AMQP.BasicProperties getProperties() {
        return properties;
    }

    // Envelope getters
    public long getDeliveryTag() {
        return envelope.getDeliveryTag();
    }

    public boolean isRedeliver() {
        return envelope.isRedeliver();
    }

    public String getExchange() {
        return envelope.getExchange();
    }

    public String getRoutingKey() {
        return envelope.getRoutingKey();
    }

    // Properties getters
    public String getContentType() {
        return properties.getContentType();
    }

    public String getContentEncoding() {
        return properties.getContentEncoding();
    }

    public Map<String, Object> getHeaders() {
        return properties.getHeaders() != null ? properties.getHeaders() : Collections.emptyMap();
    }

    public Integer getDeliveryMode() {
        return properties.getDeliveryMode();
    }

    public Integer getPriority() {
        return properties.getPriority();
    }

    public String getCorrelationId() {
        return properties.getCorrelationId();
    }

    public String getReplyTo() {
        return properties.getReplyTo();
    }

    public String getExpiration() {
        return properties.getExpiration();
    }

    public String getMessageId() {
        return properties.getMessageId();
    }

    public Date getTimestamp() {
        return properties.getTimestamp();
    }

    public String getType() {
        return properties.getType();
    }

    public String getUserId() {
        return properties.getUserId();
    }

    public String getAppId() {
        return properties.getAppId();
    }

    public String getClusterId() {
        return properties.getClusterId();
    }

    /**
     * Get the raw message body.
     */
    public byte[] getBody() {
        return body;
    }

    /**
     * Get the content type override, if set.
     */
    public String getContentTypeOverride() {
        return contentTypeOverride;
    }

    /**
     * Get the effective content type: the override if set, otherwise the content type from properties.
     */
    public Optional<String> getEffectiveContentType() {
        return Optional.ofNullable(contentTypeOverride).or(() -> Optional.ofNullable(properties.getContentType()));
    }

    /**
     * Get a header value as a string.
     */
    public Optional<String> getHeader(String key) {
        Map<String, Object> headers = getHeaders();
        Object value = headers.get(key);
        return value != null ? Optional.of(value.toString()) : Optional.empty();
    }

    /**
     * Get a header value as a specific type.
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getHeader(String key, Class<T> type) {
        Map<String, Object> headers = getHeaders();
        Object value = headers.get(key);
        if (value != null && type.isInstance(value)) {
            return Optional.of((T) value);
        }
        return Optional.empty();
    }
}
