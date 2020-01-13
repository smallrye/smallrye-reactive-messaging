package io.smallrye.reactive.messaging.amqp;

import java.time.Instant;
import java.util.UUID;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.vertx.axle.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class AmqpMessageBuilder<T> {

    private final io.vertx.axle.amqp.AmqpMessageBuilder builder;

    AmqpMessageBuilder() {
        builder = io.vertx.axle.amqp.AmqpMessage.create();
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
        io.vertx.axle.amqp.AmqpMessage delegate = builder.build();
        Metadata.MetadataBuilder hb = Metadata.builder();
        if (delegate.address() != null) {
            hb.with(AmqpMetadata.OUTGOING_ADDRESS, delegate.address());
        }
        if (delegate.applicationProperties() != null) {
            hb.with(AmqpMetadata.OUTGOING_APPLICATION_PROPERTIES, delegate.applicationProperties());
        }
        if (delegate.contentType() != null) {
            hb.with(AmqpMetadata.OUTGOING_CONTENT_TYPE, delegate.contentType());
        }
        if (delegate.contentEncoding() != null) {
            hb.with(AmqpMetadata.OUTGOING_CONTENT_ENCODING, delegate.contentEncoding());
        }
        if (delegate.correlationId() != null) {
            hb.with(AmqpMetadata.OUTGOING_CORRELATION_ID, delegate.correlationId());
        }
        if (delegate.groupId() != null) {
            hb.with(AmqpMetadata.OUTGOING_GROUP_ID, delegate.groupId());
        }
        if (delegate.id() != null) {
            hb.with(AmqpMetadata.OUTGOING_ID, delegate.id());
        }
        hb.with(AmqpMetadata.OUTGOING_DURABLE, delegate.isDurable());
        if (delegate.priority() >= 0) {
            hb.with(AmqpMetadata.OUTGOING_PRIORITY, delegate.priority());
        }
        if (delegate.subject() != null) {
            hb.with(AmqpMetadata.OUTGOING_SUBJECT, delegate.subject());
        }
        if (delegate.ttl() >= 0) {
            hb.with(AmqpMetadata.OUTGOING_TTL, delegate.ttl());
        }

        Metadata metadata = hb.build();
        return new OutgoingAmqpMessage<>(delegate, metadata);
    }
}
