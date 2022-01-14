package io.smallrye.reactive.messaging.rabbitmq;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

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

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public String getContentType() {
        return contentType;
    }

    public String getContentEncoding() {
        return contentEncoding;
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

    public static Builder builder() {
        return new Builder();
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
        public Builder withHeader(final String header, final Object value) {
            headers.put(header, value);
            return this;
        }

        /**
         * Adds the message headers to the metadata.
         *
         * @param header the message headers
         * @return this {@link Builder}
         */
        public Builder withHeaders(final Map<String, Object> header) {
            headers.putAll(header);
            return this;
        }

        /**
         * Adds an application id property to the metadata
         *
         * @param appId the application id
         * @return this {@link Builder}
         */
        public Builder withAppId(final String appId) {
            this.appId = appId;
            return this;
        }

        /**
         * Adds a content encoding property to the metadata
         *
         * @param contentEncoding the MIME content encoding
         * @return this {@link Builder}
         */
        public Builder withContentEncoding(final String contentEncoding) {
            this.contentEncoding = contentEncoding;
            return this;
        }

        /**
         * Adds a cluster id property to the metadata
         *
         * @param clusterId the cluster id
         * @return this {@link Builder}
         */
        public Builder withClusterId(final String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        /**
         * Adds a content type property to the metadata
         *
         * @param contentType the MIME content type
         * @return this {@link Builder}
         */
        public Builder withContentType(final String contentType) {
            this.contentType = contentType;
            return this;
        }

        /**
         * Adds a correlation id property to the metadata
         *
         * @param correlationId the correlation id
         * @return this {@link Builder}
         */
        public Builder withCorrelationId(final String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        /**
         * Adds a delivery mode property to the metadata
         *
         * @param deliveryMode the delivery mode; use 1 for non-persistent
         *        and 2 for persistent
         * @return this {@link Builder}
         */
        public Builder withDeliveryMode(final Integer deliveryMode) {
            this.deliveryMode = deliveryMode;
            return this;
        }

        /**
         * Adds an expiration property to the metadata
         *
         * @param expiration a string-valued representation of a time (ms)
         * @return this {@link Builder}
         */
        public Builder withExpiration(final String expiration) {
            this.expiration = expiration;
            return this;
        }

        /**
         * Adds a message id property to the metadata
         *
         * @param messageId the message id
         * @return this {@link Builder}
         */
        public Builder withMessageId(final String messageId) {
            this.messageId = messageId;
            return this;
        }

        /**
         * Adds a priority property to the metadata
         *
         * @param priority the priority (value between 0 and 9 inclusive)
         * @return this {@link Builder}
         */
        public Builder withPriority(final Integer priority) {
            this.priority = priority;
            return this;
        }

        /**
         * Adds a reply to property to the metadata
         *
         * @param replyTo the address to reply to the message
         * @return this {@link Builder}
         */
        public Builder withReplyTo(final String replyTo) {
            this.replyTo = replyTo;
            return this;
        }

        /**
         * Adds a routing key property to the metadata
         *
         * @param routingKey the routing key
         * @return this {@link Builder}
         */
        public Builder withRoutingKey(final String routingKey) {
            this.routingKey = routingKey;
            return this;
        }

        /**
         * Adds a timestamp property to the metadata
         *
         * @param timestamp a {@link ZonedDateTime} representing the timestamp
         * @return this {@link Builder}
         */
        public Builder withTimestamp(final ZonedDateTime timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        /**
         * Adds a type property to the metadata
         *
         * @param type the type
         * @return this {@link Builder}
         */
        public Builder withType(final String type) {
            this.type = type;
            return this;
        }

        /**
         * Adds a user id property to the metadata
         *
         * @param userId the user id
         * @return this {@link Builder}
         */
        public Builder withUserId(final String userId) {
            this.userId = userId;
            return this;
        }

        /**
         * Returns the built {@link OutgoingRabbitMQMetadata}.
         *
         * @return the outgoing metadata
         */
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
