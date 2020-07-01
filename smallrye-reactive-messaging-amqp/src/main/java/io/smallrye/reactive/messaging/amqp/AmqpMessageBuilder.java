package io.smallrye.reactive.messaging.amqp;

import java.time.Instant;
import java.util.UUID;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;

/**
 * @param <T>
 * @deprecated Use {@link OutgoingAmqpMetadata} instead.
 */
@Deprecated
public class AmqpMessageBuilder<T> {

    private final io.vertx.mutiny.amqp.AmqpMessageBuilder builder;

    AmqpMessageBuilder() {
        builder = io.vertx.mutiny.amqp.AmqpMessage.create();
    }

    public AmqpMessageBuilder<T> withPriority(short priority) {
        builder.priority(priority);
        return this;
    }

    public AmqpMessageBuilder<T> withDurable(boolean durable) {
        builder.durable(durable);
        return this;
    }

    public AmqpMessageBuilder<T> withTtl(long ttl) {
        builder.ttl(ttl);
        return this;
    }

    public AmqpMessageBuilder<T> withId(String id) {
        builder.id(id);
        return this;
    }

    public AmqpMessageBuilder<T> withAddress(String address) {
        builder.address(address);
        return this;
    }

    public AmqpMessageBuilder<T> withReplyTo(String replyTo) {
        builder.replyTo(replyTo);
        return this;
    }

    public AmqpMessageBuilder<T> withCorrelationId(String correlationId) {
        builder.correlationId(correlationId);
        return this;
    }

    public AmqpMessageBuilder<T> withBody(String value) {
        builder.withBody(value);
        return this;
    }

    public AmqpMessageBuilder<T> withSymbolAsBody(String value) {
        builder.withSymbolAsBody(value);
        return this;
    }

    public AmqpMessageBuilder<T> withSubject(String subject) {
        builder.subject(subject);
        return this;
    }

    public AmqpMessageBuilder<T> withContentType(String ct) {
        builder.contentType(ct);
        return this;
    }

    public AmqpMessageBuilder<T> withContentEncoding(String ct) {
        builder.contentEncoding(ct);
        return this;
    }

    public AmqpMessageBuilder<T> withGroupId(String gi) {
        builder.groupId(gi);
        return this;
    }

    public AmqpMessageBuilder<T> withReplyToGroupId(String rt) {
        builder.replyToGroupId(rt);
        return this;
    }

    public AmqpMessageBuilder<T> withApplicationProperties(JsonObject props) {
        builder.applicationProperties(props);
        return this;
    }

    public AmqpMessageBuilder<T> withBooleanAsBody(boolean v) {
        builder.withBooleanAsBody(v);
        return this;
    }

    public AmqpMessageBuilder<T> withByteAsBody(byte v) {
        builder.withByteAsBody(v);
        return this;
    }

    public AmqpMessageBuilder<T> withShortAsBody(short v) {
        builder.withShortAsBody(v);
        return this;
    }

    public AmqpMessageBuilder<T> withIntegerAsBody(int v) {
        builder.withIntegerAsBody(v);
        return this;
    }

    public AmqpMessageBuilder<T> withLongAsBody(long v) {
        builder.withLongAsBody(v);
        return this;
    }

    public AmqpMessageBuilder<T> withFloatAsBody(float v) {
        builder.withFloatAsBody(v);
        return this;
    }

    public AmqpMessageBuilder<T> withDoubleAsBody(double v) {
        builder.withDoubleAsBody(v);
        return this;
    }

    public AmqpMessageBuilder<T> withCharAsBody(char c) {
        builder.withCharAsBody(c);
        return this;
    }

    public AmqpMessageBuilder<T> withBufferAsBody(Buffer buffer) {
        builder.withBufferAsBody(buffer);
        return this;
    }

    public AmqpMessageBuilder<T> withJsonObjectAsBody(JsonObject json) {
        builder.withJsonObjectAsBody(json);
        return this;
    }

    public AmqpMessageBuilder<T> withJsonArrayAsBody(JsonArray json) {
        builder.withJsonArrayAsBody(json);
        return this;
    }

    public AmqpMessageBuilder<T> withInstantAsBody(Instant v) {
        builder.withInstantAsBody(v);
        return this;
    }

    public AmqpMessageBuilder<T> withUuidAsBody(UUID v) {
        builder.withUuidAsBody(v);
        return this;
    }

    public AmqpMessage<T> build() {
        io.vertx.mutiny.amqp.AmqpMessage delegate = builder.build();
        OutgoingAmqpMetadata.OutgoingAmqpMetadataBuilder amqpMetadataBuilder = OutgoingAmqpMetadata.builder();
        if (delegate.address() != null) {
            amqpMetadataBuilder.withAddress(delegate.address());
        }
        if (delegate.applicationProperties() != null) {
            amqpMetadataBuilder.withApplicationProperties(delegate.applicationProperties());
        }
        if (delegate.contentType() != null) {
            amqpMetadataBuilder.withContentType(delegate.contentType());
        }
        if (delegate.contentEncoding() != null) {
            amqpMetadataBuilder.withContentEncoding(delegate.contentEncoding());
        }
        if (delegate.correlationId() != null) {
            amqpMetadataBuilder.withCorrelationId(delegate.correlationId());
        }
        if (delegate.groupId() != null) {
            amqpMetadataBuilder.withGroupId(delegate.groupId());
        }
        if (delegate.id() != null) {
            amqpMetadataBuilder.withMessageId(delegate.id());
        }
        amqpMetadataBuilder.withDurable(delegate.isDurable());
        if (delegate.priority() >= 0) {
            amqpMetadataBuilder.withPriority((short) delegate.priority());
        }
        if (delegate.subject() != null) {
            amqpMetadataBuilder.withSubject(delegate.subject());
        }
        if (delegate.ttl() >= 0) {
            amqpMetadataBuilder.withTtl(delegate.ttl());
        }
        return new OutgoingAmqpMessage<>(delegate, amqpMetadataBuilder.build());
    }
}
