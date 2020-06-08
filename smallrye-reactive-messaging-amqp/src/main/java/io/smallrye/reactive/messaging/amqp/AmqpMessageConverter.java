package io.smallrye.reactive.messaging.amqp;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpMessageBuilder;
import io.vertx.mutiny.core.buffer.Buffer;

public class AmqpMessageConverter {
    private static final String JSON_CONTENT_TYPE = "application/json";

    private AmqpMessageConverter() {
        // Avoid direct instantiation.
    }

    static io.vertx.mutiny.amqp.AmqpMessage convertToAmqpMessage(Message<?> message, boolean durable, long ttl) {
        Object payload = message.getPayload();
        Optional<OutgoingAmqpMetadata> metadata = message.getMetadata(OutgoingAmqpMetadata.class);
        AmqpMessageBuilder builder = io.vertx.mutiny.amqp.AmqpMessage.create();

        if (durable) {
            builder.durable(true);
        } else {
            builder.durable(metadata.map(OutgoingAmqpMetadata::isDurable).orElse(false));
        }

        if (ttl > 0) {
            builder.ttl(ttl);
        } else {
            long t = metadata.map(OutgoingAmqpMetadata::getTtl).orElse(-1L);
            if (t > 0) {
                builder.ttl(t);
            }
        }

        if (payload instanceof String) {
            builder.withBody((String) payload);
        } else if (payload instanceof Boolean) {
            builder.withBooleanAsBody((Boolean) payload);
        } else if (payload instanceof Buffer) {
            builder.withBufferAsBody((Buffer) payload);
        } else if (payload instanceof Byte) {
            builder.withByteAsBody((Byte) payload);
        } else if (payload instanceof Character) {
            builder.withCharAsBody((Character) payload);
        } else if (payload instanceof Double) {
            builder.withDoubleAsBody((Double) payload);
        } else if (payload instanceof Float) {
            builder.withFloatAsBody((Float) payload);
        } else if (payload instanceof Instant) {
            builder.withInstantAsBody((Instant) payload);
        } else if (payload instanceof Integer) {
            builder.withIntegerAsBody((Integer) payload);
        } else if (payload instanceof JsonArray) {
            builder.withJsonArrayAsBody((JsonArray) payload)
                    .contentType(JSON_CONTENT_TYPE);
        } else if (payload instanceof JsonObject) {
            builder.withJsonObjectAsBody((JsonObject) payload)
                    .contentType(JSON_CONTENT_TYPE);
        } else if (payload instanceof Long) {
            builder.withLongAsBody((Long) payload);
        } else if (payload instanceof Short) {
            builder.withShortAsBody((Short) payload);
        } else if (payload instanceof UUID) {
            builder.withUuidAsBody((UUID) payload);
        } else if (payload instanceof byte[]) {
            builder.withBufferAsBody(Buffer.buffer((byte[]) payload));
        } else {
            builder.withBufferAsBody(new Buffer(Json.encodeToBuffer(payload)))
                    .contentType(JSON_CONTENT_TYPE);
        }

        metadata.ifPresent(new Consumer<OutgoingAmqpMetadata>() {
            @Override
            public void accept(OutgoingAmqpMetadata meta) {
                if (meta.getAddress() != null) {
                    builder.address(meta.getAddress());
                }
                if (meta.getProperties() != null && !meta.getProperties().isEmpty()) {
                    builder.applicationProperties(meta.getProperties());
                }
                if (meta.getContentEncoding() != null) {
                    builder.contentEncoding(meta.getContentEncoding());
                }
                if (meta.getContentType() != null) {
                    builder.contentType(meta.getContentType());
                }
                if (meta.getCorrelationId() != null) {
                    builder.correlationId(meta.getCorrelationId());
                }
                if (meta.getId() != null) {
                    builder.id(meta.getId());
                }
                if (meta.getGroupId() != null) {
                    builder.groupId(meta.getGroupId());
                }
                if (meta.getPriority() >= 0) {
                    builder.priority((short) meta.getPriority());
                }
                if (meta.getSubject() != null) {
                    builder.subject(meta.getSubject());
                }
            }
        });
        return builder.build();
    }
}
