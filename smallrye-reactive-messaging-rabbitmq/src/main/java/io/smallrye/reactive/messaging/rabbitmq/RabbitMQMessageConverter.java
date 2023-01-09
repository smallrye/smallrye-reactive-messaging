package io.smallrye.reactive.messaging.rabbitmq;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.eclipse.microprofile.reactive.messaging.Message;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.smallrye.reactive.messaging.rabbitmq.tracing.RabbitMQTrace;
import io.smallrye.reactive.messaging.tracing.TracingUtils;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.rabbitmq.RabbitMQMessage;

/**
 * Utility class which can handle the transformation of a {@link Message}
 * to an {@link OutgoingRabbitMQMessage}.
 */
public class RabbitMQMessageConverter {

    private static final List<Class<?>> PRIMITIVES = Arrays.asList(
            String.class,
            UUID.class,
            Boolean.class,
            Byte.class,
            Character.class,
            Short.class,
            Integer.class,
            Double.class,
            Float.class,
            Long.class);

    private RabbitMQMessageConverter() {
        // Avoid direct instantiation.
    }

    /**
     * Converts the supplied {@link Message} to an {@link OutgoingRabbitMQMessage}.
     *
     * @param message the source message
     * @param exchange the destination exchange
     * @param defaultRoutingKey the fallback routing key to use
     * @param isTracingEnabled whether tracing is enabled
     * @return an {@link OutgoingRabbitMQMessage}
     */
    public static OutgoingRabbitMQMessage convert(
            final Instrumenter<RabbitMQTrace, Void> instrumenter,
            final Message<?> message,
            final String exchange,
            final String defaultRoutingKey,
            final Optional<Long> defaultTtl,
            final boolean isTracingEnabled) {
        final Optional<io.vertx.mutiny.rabbitmq.RabbitMQMessage> rabbitMQMessage = getRabbitMQMessage(message);
        final String routingKey = getRoutingKey(message).orElse(defaultRoutingKey);

        // Figure out the body and properties
        Buffer body;
        BasicProperties properties;

        if (rabbitMQMessage.isPresent()) {
            // If we already have a RabbitMQMessage present, use it as the basis for the outgoing one
            body = rabbitMQMessage.get().body();
            final BasicProperties sourceProperties = rabbitMQMessage.get().properties();
            // Make a copy of the source headers as the original is probably immutable
            final Map<String, Object> sourceHeaders = new HashMap<>(sourceProperties.getHeaders());

            if (isTracingEnabled) {
                // Create a new span for the outbound message and record updated tracing information in
                // the headers; this has to be done before we build the properties below
                TracingUtils.traceOutgoing(instrumenter, message, RabbitMQTrace.trace(exchange, sourceHeaders));
            }

            // Reconstruct the properties from the source, except with the (possibly) modified headers;
            // only override the existing expiration if not already set and a non-negative default TTL
            // has been specified.
            final String expiration = (null != sourceProperties.getExpiration()) ? sourceProperties.getExpiration()
                    : defaultTtl.map(String::valueOf).orElse(null);

            // If not already specified, figure out the content type from the message payload
            final String contentType = (sourceProperties.getContentType() != null) ? sourceProperties.getContentType()
                    : getDefaultContentTypeForPayload(message.getPayload());

            properties = new AMQP.BasicProperties.Builder()
                    .contentType(contentType)
                    .contentEncoding(sourceProperties.getContentEncoding())
                    .headers(sourceHeaders)
                    .deliveryMode(sourceProperties.getDeliveryMode())
                    .priority(sourceProperties.getPriority())
                    .correlationId(sourceProperties.getCorrelationId())
                    .replyTo(sourceProperties.getReplyTo())
                    .expiration(expiration)
                    .messageId(sourceProperties.getMessageId())
                    .timestamp(sourceProperties.getTimestamp())
                    .type(sourceProperties.getType())
                    .userId(sourceProperties.getUserId())
                    .appId(sourceProperties.getAppId())
                    .build();
        } else {
            // Getting here means we have to work a little harder
            final String defaultContentType = getDefaultContentTypeForPayload(message.getPayload());
            body = getBodyFromPayload(message.getPayload());

            final OutgoingRabbitMQMetadata metadata = message.getMetadata(OutgoingRabbitMQMetadata.class)
                    .orElse(new OutgoingRabbitMQMetadata.Builder()
                            .withContentType(defaultContentType)
                            .withExpiration(defaultTtl.map(String::valueOf).orElse(null))
                            .build());

            if (isTracingEnabled) {
                // Create a new span for the outbound message and record updated tracing information in
                // the message headers; this has to be done before we build the properties below
                TracingUtils.traceOutgoing(instrumenter, message, RabbitMQTrace.trace(exchange, metadata.getHeaders()));
            }

            final Date timestamp = (metadata.getTimestamp() != null) ? Date.from(metadata.getTimestamp().toInstant()) : null;

            // If not already specified, use the default content type for the message payload
            final String contentType = (metadata.getContentType() != null) ? metadata.getContentType()
                    : defaultContentType;

            properties = new AMQP.BasicProperties.Builder()
                    .contentType(contentType)
                    .contentEncoding(metadata.getContentEncoding())
                    .headers(metadata.getHeaders())
                    .deliveryMode(metadata.getDeliveryMode())
                    .priority(metadata.getPriority())
                    .correlationId(metadata.getCorrelationId())
                    .replyTo(metadata.getReplyTo())
                    .expiration(metadata.getExpiration())
                    .messageId(metadata.getMessageId())
                    .timestamp(timestamp)
                    .type(metadata.getType())
                    .userId(metadata.getUserId())
                    .appId(metadata.getAppId())
                    .clusterId(metadata.getClusterId())
                    .build();
        }

        return new OutgoingRabbitMQMessage(routingKey, body, properties);
    }

    /**
     * Returns a {@link Buffer} containing the supplied payload.
     *
     * @param payload the payload
     * @return a buffer encapsulation of the payload
     */
    private static Buffer getBodyFromPayload(final Object payload) {
        if (isPrimitive(payload.getClass())) {
            // Anything representable as a string is rendered as a String
            return Buffer.buffer(payload.toString());
        } else if (payload instanceof Buffer) {
            return (Buffer) payload;
        } else if (payload instanceof io.vertx.core.buffer.Buffer) {
            return Buffer.buffer(((io.vertx.core.buffer.Buffer) payload).getBytes());
        } else if (payload instanceof byte[]) {
            return Buffer.buffer((byte[]) payload);
        } else if (payload instanceof JsonObject) {
            return Buffer.buffer(((JsonObject) payload).encode());
        } else if (payload instanceof JsonArray) {
            return Buffer.buffer(((JsonArray) payload).encode());
        } else {
            // Other objects are serialized to JSON
            return Buffer.buffer(Json.encode(payload));
        }
    }

    /**
     * Returns the default content type based on the class of the payload.
     *
     * @param payload the payload
     * @return the default content typ
     */
    private static String getDefaultContentTypeForPayload(final Object payload) {
        if (isPrimitive(payload.getClass())) {
            // Anything representable a string is rendered as a String
            return HttpHeaderValues.TEXT_PLAIN.toString();
        } else if (payload instanceof Buffer) {
            return HttpHeaderValues.APPLICATION_OCTET_STREAM.toString();
        } else if (payload instanceof io.vertx.core.buffer.Buffer) {
            return HttpHeaderValues.APPLICATION_OCTET_STREAM.toString();
        } else if (payload instanceof byte[]) {
            return HttpHeaderValues.APPLICATION_OCTET_STREAM.toString();
        } else if (payload instanceof JsonObject) {
            return HttpHeaderValues.APPLICATION_JSON.toString();
        } else if (payload instanceof JsonArray) {
            return HttpHeaderValues.APPLICATION_JSON.toString();
        } else {
            // Other objects are serialized to JSON
            return HttpHeaderValues.APPLICATION_JSON.toString();
        }
    }

    private static Optional<RabbitMQMessage> getRabbitMQMessage(final Message<?> message) {
        if (message instanceof IncomingRabbitMQMessage) {
            return Optional.of(((IncomingRabbitMQMessage<?>) message)
                    .getRabbitMQMessage());
        } else if (message.getPayload() instanceof io.vertx.mutiny.rabbitmq.RabbitMQMessage) {
            return Optional.of((io.vertx.mutiny.rabbitmq.RabbitMQMessage) message.getPayload());
        } else if (message.getPayload() instanceof io.vertx.rabbitmq.RabbitMQMessage) {
            return Optional.of(new io.vertx.mutiny.rabbitmq.RabbitMQMessage(
                    (io.vertx.rabbitmq.RabbitMQMessage) message.getPayload()));
        } else {
            return Optional.empty();
        }
    }

    private static Optional<String> getRoutingKey(final Message<?> message) {
        final Optional<io.vertx.mutiny.rabbitmq.RabbitMQMessage> rabbitMQMessage = getRabbitMQMessage(message);

        if (rabbitMQMessage.isPresent()) {
            return Optional.of(rabbitMQMessage.get().envelope().getRoutingKey());
        }

        // Getting here means we have to work a little harder
        final OutgoingRabbitMQMetadata metadata = message.getMetadata(OutgoingRabbitMQMetadata.class)
                .orElse(new OutgoingRabbitMQMetadata());
        return Optional.ofNullable(metadata.getRoutingKey());
    }

    private static boolean isPrimitive(Class<?> clazz) {
        return clazz.isPrimitive() || PRIMITIVES.contains(clazz);
    }

    /**
     * Represents an outgoing RabbitMQ message.
     */
    public static final class OutgoingRabbitMQMessage {
        private final String routingKey;
        private final Buffer body;
        private final BasicProperties properties;

        /**
         * Constructor.
         *
         * @param routingKey the routing key for the message
         * @param body the message body
         * @param properties the message properties
         */
        private OutgoingRabbitMQMessage(
                final String routingKey,
                final Buffer body,
                final BasicProperties properties) {
            this.routingKey = routingKey;
            this.body = body;
            this.properties = properties;
        }

        /**
         * The body of this message.
         *
         * @return the body
         */
        public Buffer getBody() {
            return this.body;
        }

        /**
         * The routing key for this message.
         *
         * @return the routing key
         */
        public String getRoutingKey() {
            return this.routingKey;
        }

        /**
         * The properties for this message.
         *
         * @return the properties
         */
        public BasicProperties getProperties() {
            return properties;
        }
    }

}
