package io.smallrye.reactive.messaging.amqp;

import java.sql.Date;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.vertx.amqp.impl.AmqpMessageImpl;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpMessage;
import io.vertx.mutiny.core.buffer.Buffer;

public class AmqpMessageConverter {
    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final String BINARY_CONTENT_TYPE = "application/octet-stream";

    private AmqpMessageConverter() {
        // Avoid direct instantiation.
    }

    static io.vertx.mutiny.amqp.AmqpMessage convertToAmqpMessage(Message<?> message, boolean durable, long ttl) {
        Object payload = message.getPayload();
        OutgoingAmqpMetadata metadata = message.getMetadata(OutgoingAmqpMetadata.class)
                .orElse(new OutgoingAmqpMetadata());

        org.apache.qpid.proton.message.Message output = org.apache.qpid.proton.message.Message.Factory.create();

        // Header
        if (metadata.isDurable()) {
            output.setDurable(true);
        } else {
            output.setDurable(durable);
        }

        output.setPriority(metadata.getPriority());

        if (metadata.getTtl() > 0) {
            output.setTtl(metadata.getTtl());
        } else if (ttl > 0) {
            output.setTtl(ttl);
        }

        // Annotations
        DeliveryAnnotations deliveryAnnotations = metadata.getDeliveryAnnotations();
        MessageAnnotations messageAnnotations = metadata.getMessageAnnotations();
        if (!deliveryAnnotations.getValue().isEmpty()) {
            output.setDeliveryAnnotations(deliveryAnnotations);
        }
        if (!messageAnnotations.getValue().isEmpty()) {
            output.setMessageAnnotations(messageAnnotations);
        }

        // Properties
        output.setMessageId(metadata.getMessageId());
        output.setUserId(metadata.getUserId() != null ? metadata.getUserId().getBytes() : null);
        output.setAddress(metadata.getAddress());
        output.setSubject(metadata.getSubject());
        output.setReplyTo(metadata.getReplyTo());
        output.setCorrelationId(metadata.getCorrelationId());
        output.setContentType(metadata.getContentType());
        output.setContentEncoding(metadata.getContentEncoding());
        output.setExpiryTime(metadata.getExpiryTime());
        output.setCreationTime(metadata.getCreationTime());
        output.setGroupId(metadata.getGroupId());
        output.setGroupSequence(metadata.getGroupSequence());
        output.setReplyToGroupId(metadata.getReplyToGroupId());

        if (!metadata.getProperties().isEmpty()) {
            output.setApplicationProperties(new ApplicationProperties(metadata.getProperties().getMap()));
        }

        // Application data section:
        if (payload == null) {
            output.setBody(new AmqpValue(null));
        } else if (payload instanceof String || isPrimitive(payload.getClass()) || payload instanceof UUID) {
            output.setBody(new AmqpValue(payload));

        } else if (payload instanceof Buffer) {
            output.setBody(new Data(new Binary(((Buffer) payload).getBytes())));
            if (output.getContentType() == null) {
                output.setContentType(BINARY_CONTENT_TYPE);
            }
        } else if (payload instanceof io.vertx.core.buffer.Buffer) {
            output.setBody(new Data(new Binary(((io.vertx.core.buffer.Buffer) payload).getBytes())));
            if (output.getContentType() == null) {
                output.setContentType(BINARY_CONTENT_TYPE);
            }
        } else if (payload instanceof Instant) {
            output.setBody(new AmqpValue(Date.from((Instant) payload)));
        } else if (payload instanceof JsonArray) {
            byte[] bytes = ((JsonArray) payload).toBuffer().getBytes();
            output.setBody(new Data(new Binary(bytes)));
            if (output.getContentType() == null) {
                output.setContentType(JSON_CONTENT_TYPE);
            }
        } else if (payload instanceof JsonObject) {
            byte[] bytes = ((JsonObject) payload).toBuffer().getBytes();
            output.setBody(new Data(new Binary(bytes)));
            if (output.getContentType() == null) {
                output.setContentType(JSON_CONTENT_TYPE);
            }
        } else if (payload instanceof byte[]) {
            output.setBody(new Data(new Binary(((byte[]) payload))));
            if (output.getContentType() == null) {
                output.setContentType(BINARY_CONTENT_TYPE);
            }
        } else if (payload instanceof Map || payload instanceof List) {
            // This branch must be after the JSON Object and JSON Array checks
            output.setBody(new AmqpValue(payload));
        } else {
            byte[] bytes = Json.encodeToBuffer(payload).getBytes();
            output.setBody(new Data(new Binary(bytes)));
            if (output.getContentType() == null) {
                output.setContentType(JSON_CONTENT_TYPE);
            }
        }

        // Footer
        Footer footer = metadata.getFooter();
        if (!footer.getValue().isEmpty()) {
            output.setFooter(footer);
        }

        return new AmqpMessage(new AmqpMessageImpl(output));
    }

    private static final List<Class<?>> PRIMITIVES = Arrays.asList(
            Boolean.class,
            Byte.class,
            Character.class,
            Short.class,
            Integer.class,
            Double.class,
            Float.class,
            Long.class);

    private static boolean isPrimitive(Class<?> clazz) {
        return clazz.isPrimitive() || PRIMITIVES.contains(clazz);
    }
}
