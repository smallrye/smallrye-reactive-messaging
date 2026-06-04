package io.smallrye.reactive.messaging.rabbitmq.og;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.eclipse.microprofile.reactive.messaging.Message;

import com.rabbitmq.client.AMQP;

import io.vertx.core.json.JsonObject;

/**
 * Utility class for converting between Reactive Messaging Message and RabbitMQ message format.
 * Handles different payload types and content-type determination.
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

    private static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";
    private static final String CONTENT_TYPE_APPLICATION_JSON = "application/json";
    private static final String CONTENT_TYPE_APPLICATION_OCTET_STREAM = "application/octet-stream";

    private RabbitMQMessageConverter() {
        // Utility class - no instantiation
    }

    /**
     * Converts the supplied Message to RabbitMQ message components.
     *
     * @param message the source message
     * @param defaultRoutingKey the fallback routing key
     * @param defaultTtl optional default TTL
     * @return the converted message components
     */
    public static OutgoingRabbitMQMessage convert(
            final Message<?> message,
            final String defaultRoutingKey,
            final Optional<Long> defaultTtl) {

        // Check if message is already an IncomingRabbitMQMessage
        if (message instanceof IncomingRabbitMQMessage) {
            return convertFromIncoming((IncomingRabbitMQMessage<?>) message, defaultRoutingKey);
        }

        // Convert payload to bytes
        byte[] body = getBodyFromPayload(message.getPayload());
        String defaultContentType = getDefaultContentTypeForPayload(message.getPayload());

        Optional<OutgoingRabbitMQMetadata> outgoing = message.getMetadata(OutgoingRabbitMQMetadata.class);
        OutgoingRabbitMQMetadata.Builder builder = outgoing.map(out -> {
            OutgoingRabbitMQMetadata.Builder b = OutgoingRabbitMQMetadata.from(out);
            if (out.getProperties().getContentType() == null) {
                b.withContentType(defaultContentType);
            }
            if (out.getProperties().getDeliveryMode() == null) {
                b.withDeliveryMode(2);
            }
            return b;
        }).orElseGet(() -> OutgoingRabbitMQMetadata.builder()
                .withContentType(defaultContentType)
                .withDeliveryMode(2)
                .withExpiration(defaultTtl.map(String::valueOf).orElse(null)));

        Optional<IncomingRabbitMQMetadata> incoming = message.getMetadata(IncomingRabbitMQMetadata.class);
        incoming.ifPresent(in -> {
            if (outgoing.map(m -> m.getProperties().getCorrelationId()).isEmpty()) {
                String cid = in.getCorrelationId();
                if (cid != null) {
                    builder.withCorrelationId(cid);
                }
            }
        });

        OutgoingRabbitMQMetadata metadata = builder.build();

        // Get routing key: outgoing metadata > replyTo from incoming > default
        String routingKey = metadata.getRoutingKey();
        if (routingKey == null) {
            routingKey = incoming.map(IncomingRabbitMQMetadata::getReplyTo).orElse(null);
        }
        if (routingKey == null) {
            routingKey = defaultRoutingKey;
        }

        // Get exchange from metadata (optional override)
        Optional<String> exchange = metadata.getExchange();

        return new OutgoingRabbitMQMessage(routingKey, exchange, body, metadata.getProperties());
    }

    /**
     * Convert from an IncomingRabbitMQMessage (forwarding scenario).
     */
    private static OutgoingRabbitMQMessage convertFromIncoming(
            IncomingRabbitMQMessage<?> incomingMessage,
            String defaultRoutingKey) {

        IncomingRabbitMQMetadata metadata = incomingMessage.getRabbitMQMetadata();

        // Use original routing key or default
        String routingKey = metadata.getRoutingKey() != null ? metadata.getRoutingKey() : defaultRoutingKey;

        // Use original exchange
        Optional<String> exchange = Optional.ofNullable(metadata.getExchange());

        // Get payload as bytes
        byte[] body;
        Object payload = incomingMessage.getPayload();
        if (payload instanceof byte[]) {
            body = (byte[]) payload;
        } else if (payload instanceof String) {
            body = ((String) payload).getBytes(StandardCharsets.UTF_8);
        } else {
            body = getBodyFromPayload(payload);
        }

        // Copy properties from incoming message
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType(metadata.getContentType())
                .contentEncoding(metadata.getContentEncoding())
                .headers(metadata.getHeaders())
                .deliveryMode(metadata.getDeliveryMode())
                .priority(metadata.getPriority())
                .correlationId(metadata.getCorrelationId())
                .replyTo(metadata.getReplyTo())
                .expiration(metadata.getExpiration())
                .messageId(metadata.getMessageId())
                .timestamp(metadata.getTimestamp())
                .type(metadata.getType())
                .userId(metadata.getUserId())
                .appId(metadata.getAppId())
                .build();

        return new OutgoingRabbitMQMessage(routingKey, exchange, body, properties);
    }

    /**
     * Convert payload to byte array.
     */
    private static byte[] getBodyFromPayload(Object payload) {
        if (payload == null) {
            return new byte[0];
        }

        if (payload instanceof byte[]) {
            return (byte[]) payload;
        }

        if (payload instanceof String) {
            return ((String) payload).getBytes(StandardCharsets.UTF_8);
        }

        if (isPrimitive(payload.getClass())) {
            return payload.toString().getBytes(StandardCharsets.UTF_8);
        }

        // For complex objects, serialize to JSON
        return JsonObject.mapFrom(payload).encode().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Determine default content-type based on payload type.
     */
    private static String getDefaultContentTypeForPayload(Object payload) {
        if (payload == null) {
            return CONTENT_TYPE_APPLICATION_OCTET_STREAM;
        }

        if (payload instanceof byte[]) {
            return CONTENT_TYPE_APPLICATION_OCTET_STREAM;
        }

        if (payload instanceof String) {
            return CONTENT_TYPE_TEXT_PLAIN;
        }

        if (isPrimitive(payload.getClass())) {
            return CONTENT_TYPE_TEXT_PLAIN;
        }

        // Default to JSON for complex objects
        return CONTENT_TYPE_APPLICATION_JSON;
    }

    /**
     * Check if class is a primitive or wrapper type.
     */
    private static boolean isPrimitive(Class<?> clazz) {
        return clazz.isPrimitive() || PRIMITIVES.contains(clazz);
    }

    /**
     * Represents an outgoing RabbitMQ message.
     */
    public static final class OutgoingRabbitMQMessage {
        private final String routingKey;
        private final Optional<String> exchange;
        private final byte[] body;
        private final AMQP.BasicProperties properties;

        private OutgoingRabbitMQMessage(
                String routingKey,
                Optional<String> exchange,
                byte[] body,
                AMQP.BasicProperties properties) {
            this.routingKey = routingKey;
            this.exchange = exchange;
            this.body = body;
            this.properties = properties;
        }

        public String getRoutingKey() {
            return routingKey;
        }

        public Optional<String> getExchange() {
            return exchange;
        }

        public byte[] getBody() {
            return body;
        }

        public AMQP.BasicProperties getProperties() {
            return properties;
        }
    }
}
