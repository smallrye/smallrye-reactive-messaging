package io.smallrye.reactive.messaging.rabbitmq.og;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.rabbitmq.client.AMQP;

/**
 * Metadata for outgoing RabbitMQ messages.
 * Allows customization of routing, properties, and message attributes.
 */
public class OutgoingRabbitMQMetadata {

    private final String routingKey;
    private final String exchange;
    private final AMQP.BasicProperties properties;

    private OutgoingRabbitMQMetadata(String routingKey, String exchange, AMQP.BasicProperties properties) {
        this.routingKey = routingKey;
        this.exchange = exchange;
        this.properties = properties;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public Optional<String> getExchange() {
        return Optional.ofNullable(exchange);
    }

    public AMQP.BasicProperties getProperties() {
        return properties;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String routingKey;
        private String exchange;
        private String contentType;
        private String contentEncoding;
        private Map<String, Object> headers = new HashMap<>();
        private Integer deliveryMode;
        private Integer priority;
        private String correlationId;
        private String replyTo;
        private String expiration;
        private String messageId;
        private Date timestamp;
        private String type;
        private String userId;
        private String appId;

        public Builder withRoutingKey(String routingKey) {
            this.routingKey = routingKey;
            return this;
        }

        public Builder withExchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        public Builder withContentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder withContentEncoding(String contentEncoding) {
            this.contentEncoding = contentEncoding;
            return this;
        }

        public Builder withHeader(String key, Object value) {
            this.headers.put(key, value);
            return this;
        }

        public Builder withHeaders(Map<String, Object> headers) {
            if (headers != null) {
                this.headers.putAll(headers);
            }
            return this;
        }

        /**
         * Set delivery mode: 1 for transient, 2 for persistent.
         */
        public Builder withDeliveryMode(Integer deliveryMode) {
            this.deliveryMode = deliveryMode;
            return this;
        }

        /**
         * Set as persistent message (delivery mode 2).
         */
        public Builder withPersistent(boolean persistent) {
            this.deliveryMode = persistent ? 2 : 1;
            return this;
        }

        /**
         * Set message priority (0-9, higher is more priority).
         */
        public Builder withPriority(Integer priority) {
            this.priority = priority;
            return this;
        }

        public Builder withCorrelationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder withReplyTo(String replyTo) {
            this.replyTo = replyTo;
            return this;
        }

        /**
         * Set per-message TTL in milliseconds as a string.
         */
        public Builder withExpiration(String expiration) {
            this.expiration = expiration;
            return this;
        }

        /**
         * Set per-message TTL in milliseconds.
         */
        public Builder withTtl(long ttlMs) {
            this.expiration = String.valueOf(ttlMs);
            return this;
        }

        public Builder withMessageId(String messageId) {
            this.messageId = messageId;
            return this;
        }

        public Builder withTimestamp(Date timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder withType(String type) {
            this.type = type;
            return this;
        }

        public Builder withUserId(String userId) {
            this.userId = userId;
            return this;
        }

        public Builder withAppId(String appId) {
            this.appId = appId;
            return this;
        }

        public OutgoingRabbitMQMetadata build() {
            AMQP.BasicProperties.Builder propsBuilder = new AMQP.BasicProperties.Builder();

            if (contentType != null) {
                propsBuilder.contentType(contentType);
            }
            if (contentEncoding != null) {
                propsBuilder.contentEncoding(contentEncoding);
            }
            if (!headers.isEmpty()) {
                propsBuilder.headers(headers);
            }
            if (deliveryMode != null) {
                propsBuilder.deliveryMode(deliveryMode);
            }
            if (priority != null) {
                propsBuilder.priority(priority);
            }
            if (correlationId != null) {
                propsBuilder.correlationId(correlationId);
            }
            if (replyTo != null) {
                propsBuilder.replyTo(replyTo);
            }
            if (expiration != null) {
                propsBuilder.expiration(expiration);
            }
            if (messageId != null) {
                propsBuilder.messageId(messageId);
            }
            if (timestamp != null) {
                propsBuilder.timestamp(timestamp);
            }
            if (type != null) {
                propsBuilder.type(type);
            }
            if (userId != null) {
                propsBuilder.userId(userId);
            }
            if (appId != null) {
                propsBuilder.appId(appId);
            }

            return new OutgoingRabbitMQMetadata(routingKey, exchange, propsBuilder.build());
        }
    }
}
