package io.smallrye.reactive.messaging.rabbitmq;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import io.vertx.codegen.annotations.Fluent;

/**
 * Used to represent RabbitMQ metadata in on outgoing message.
 */
public class OutgoingRabbitMQMetadata {

    private Integer priority;
    private String contentType;
    private String contentEncoding;
    private final Map<String, Object> headers = new HashMap<>();
    private Integer deliveryMode;
    private String correlationId;
    private String replyTo;
    private String expiration;
    private String messageId;
    private ZonedDateTime timestamp;
    private String type;
    private String userId;
    private String appId;
    private String clusterId;
    private String routingKey;

    public String getContentType() {
        return contentType;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public Integer getDeliveryMode() {
        return deliveryMode;
    }

    public Integer getPriority() {
        return priority;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getReplyTo() {
        return replyTo;
    }

    public String getExpiration() {
        return expiration;
    }

    public String getMessageId() {
        return messageId;
    }

    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    public String getType() {
        return type;
    }

    public String getUserId() {
        return userId;
    }

    public String getAppId() {
        return appId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    /**
     * Allows the builder-style construction of {@link OutgoingRabbitMQMetadata}
     */
    public static class Builder {
        private Integer priority;
        private String contentType;
        private String contentEncoding;
        private Map<String, Object> headers = new HashMap<>();
        private Integer deliveryMode;
        private String correlationId;
        private String replyTo;
        private String expiration;
        private String messageId;
        private ZonedDateTime timestamp;
        private String type;
        private String userId;
        private String appId;
        private String clusterId;
        private String routingKey;

        /**
         * Adds a message header.
         * 
         * @param header the header name
         * @param value the header value
         * @return this {@link Builder}
         */
        @Fluent
        public Builder withHeader(final String header, final Object value) {
            headers.put(header, value);
            return this;
        }

        @Fluent
        public Builder withAppId(final String appId) {
            this.appId = appId;
            return this;
        }

        @Fluent
        public Builder withContentEncoding(final String contentEncoding) {
            this.contentEncoding = contentEncoding;
            return this;
        }

        @Fluent
        public Builder withClusterId(final String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        @Fluent
        public Builder withContentType(final String contentType) {
            this.contentType = contentType;
            return this;
        }

        @Fluent
        public Builder withCorrelationId(final String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        @Fluent
        public Builder withDeliveryMode(final Integer deliveryMode) {
            this.deliveryMode = deliveryMode;
            return this;
        }

        @Fluent
        public Builder withExpiration(final String expiration) {
            this.expiration = expiration;
            return this;
        }

        @Fluent
        public Builder withMessageId(final String messageId) {
            this.messageId = messageId;
            return this;
        }

        @Fluent
        public Builder withPriority(final Integer priority) {
            this.priority = priority;
            return this;
        }

        @Fluent
        public Builder withReplyTo(final String replyTo) {
            this.replyTo = replyTo;
            return this;
        }

        @Fluent
        public Builder withRoutingKey(final String routingKey) {
            this.routingKey = routingKey;
            return this;
        }

        @Fluent
        public Builder withTimestamp(final ZonedDateTime timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        @Fluent
        public Builder withType(final String type) {
            this.type = type;
            return this;
        }

        @Fluent
        public Builder withUserId(final String userId) {
            this.userId = userId;
            return this;
        }

        public OutgoingRabbitMQMetadata build() {
            final OutgoingRabbitMQMetadata metadata = new OutgoingRabbitMQMetadata();
            metadata.appId = appId;
            metadata.contentEncoding = contentEncoding;
            metadata.clusterId = clusterId;
            metadata.correlationId = correlationId;
            metadata.headers.putAll(headers);
            metadata.contentType = contentType;
            metadata.correlationId = correlationId;
            metadata.deliveryMode = deliveryMode;
            metadata.expiration = expiration;
            metadata.messageId = messageId;
            metadata.priority = priority;
            metadata.replyTo = replyTo;
            metadata.routingKey = routingKey;
            metadata.timestamp = timestamp;
            metadata.type = type;
            metadata.userId = userId;
            return metadata;
        }
    }
}
