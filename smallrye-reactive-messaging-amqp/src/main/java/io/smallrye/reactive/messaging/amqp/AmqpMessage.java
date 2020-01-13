package io.smallrye.reactive.messaging.amqp;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.MessageError;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.vertx.axle.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class AmqpMessage<T> implements org.eclipse.microprofile.reactive.messaging.Message<T> {

    protected final io.vertx.amqp.AmqpMessage message;
    protected final Metadata metadata;

    public static <T> AmqpMessageBuilder<T> builder() {
        return new AmqpMessageBuilder<>();
    }

    public AmqpMessage(io.vertx.axle.amqp.AmqpMessage delegate) {
        this.message = delegate.getDelegate();
        Metadata.MetadataBuilder builder = Metadata.builder();

        if (delegate.address() != null) {
            builder.with(AmqpMetadata.ADDRESS, delegate.address());
        }
        if (delegate.applicationProperties() != null) {
            builder.with(AmqpMetadata.APPLICATION_PROPERTIES, delegate.applicationProperties());
        }
        if (delegate.contentType() != null) {
            builder.with(AmqpMetadata.CONTENT_TYPE, delegate.contentType());
        }
        if (delegate.contentEncoding() != null) {
            builder.with(AmqpMetadata.CONTENT_ENCODING, delegate.contentEncoding());
        }
        if (delegate.correlationId() != null) {
            builder.with(AmqpMetadata.CORRELATION_ID, delegate.correlationId());
        }
        if (delegate.creationTime() > 0) {
            builder.with(AmqpMetadata.CREATION_TIME, delegate.creationTime());
        }
        if (delegate.deliveryCount() >= 0) {
            builder.with(AmqpMetadata.DELIVERY_COUNT, delegate.deliveryCount());
        }
        if (delegate.expiryTime() >= 0) {
            builder.with(AmqpMetadata.EXPIRY_TIME, delegate.expiryTime());
        }
        if (delegate.groupId() != null) {
            builder.with(AmqpMetadata.GROUP_ID, delegate.groupId());
        }
        if (delegate.groupSequence() >= 0) {
            builder.with(AmqpMetadata.GROUP_SEQUENCE, delegate.groupSequence());
        }
        if (delegate.id() != null) {
            builder.with(AmqpMetadata.ID, delegate.id());
        }
        builder.with(AmqpMetadata.DURABLE, delegate.isDurable());
        builder.with(AmqpMetadata.FIRST_ACQUIRER, delegate.isFirstAcquirer());
        if (delegate.priority() >= 0) {
            builder.with(AmqpMetadata.PRIORITY, delegate.priority());
        }
        if (delegate.subject() != null) {
            builder.with(AmqpMetadata.SUBJECT, delegate.subject());
        }
        if (delegate.ttl() >= 0) {
            builder.with(AmqpMetadata.TTL, delegate.ttl());
        }
        if (message.unwrap().getHeader() != null) {
            builder.with(AmqpMetadata.HEADER, message.unwrap().getHeader());
        }
        this.metadata = builder.build();
    }

    public AmqpMessage(io.vertx.amqp.AmqpMessage msg) {
        this.message = msg;
        Metadata.MetadataBuilder builder = Metadata.builder();

        if (msg.address() != null) {
            builder.with(AmqpMetadata.ADDRESS, msg.address());
        }
        if (msg.applicationProperties() != null) {
            builder.with(AmqpMetadata.APPLICATION_PROPERTIES, msg.applicationProperties());
        }
        if (msg.contentType() != null) {
            builder.with(AmqpMetadata.CONTENT_TYPE, msg.contentType());
        }
        if (msg.contentEncoding() != null) {
            builder.with(AmqpMetadata.CONTENT_ENCODING, msg.contentEncoding());
        }
        if (msg.correlationId() != null) {
            builder.with(AmqpMetadata.CORRELATION_ID, msg.correlationId());
        }
        if (msg.creationTime() > 0) {
            builder.with(AmqpMetadata.CREATION_TIME, msg.creationTime());
        }
        if (msg.deliveryCount() >= 0) {
            builder.with(AmqpMetadata.DELIVERY_COUNT, msg.deliveryCount());
        }
        if (msg.expiryTime() >= 0) {
            builder.with(AmqpMetadata.EXPIRY_TIME, msg.expiryTime());
        }
        if (msg.groupId() != null) {
            builder.with(AmqpMetadata.GROUP_ID, msg.groupId());
        }
        if (msg.groupSequence() >= 0) {
            builder.with(AmqpMetadata.GROUP_SEQUENCE, msg.groupSequence());
        }
        if (msg.id() != null) {
            builder.with(AmqpMetadata.ID, msg.id());
        }
        builder.with(AmqpMetadata.DURABLE, msg.isDurable());
        builder.with(AmqpMetadata.FIRST_ACQUIRER, msg.isFirstAcquirer());
        if (msg.priority() >= 0) {
            builder.with(AmqpMetadata.PRIORITY, msg.priority());
        }
        if (msg.subject() != null) {
            builder.with(AmqpMetadata.SUBJECT, msg.subject());
        }
        if (msg.ttl() >= 0) {
            builder.with(AmqpMetadata.TTL, msg.ttl());
        }
        if (message.unwrap().getHeader() != null) {
            builder.with(AmqpMetadata.HEADER, message.unwrap().getHeader());
        }
        this.metadata = builder.build();
    }

    @Override
    public CompletionStage<Void> ack() {
        this.message.accepted();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public T getPayload() {
        return (T) convert(message);
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    private Object convert(io.vertx.amqp.AmqpMessage msg) {
        Object body = msg.unwrap().getBody();
        if (body instanceof AmqpValue) {
            Object value = ((AmqpValue) body).getValue();
            if (value instanceof Binary) {
                Binary bin = (Binary) value;
                byte[] bytes = new byte[bin.getLength()];
                System.arraycopy(bin.getArray(), bin.getArrayOffset(), bytes, 0, bin.getLength());
                return bytes;
            }
            return value;
        }

        if (body instanceof AmqpSequence) {
            List list = ((AmqpSequence) body).getValue();
            return list;
        }

        if (body instanceof Data) {
            Binary bin = ((Data) body).getValue();
            byte[] bytes = new byte[bin.getLength()];
            System.arraycopy(bin.getArray(), bin.getArrayOffset(), bytes, 0, bin.getLength());

            if ("application/json".equalsIgnoreCase(msg.contentType())) {
                return Buffer.buffer(bytes).toJson();
            }
            return bytes;
        }

        return body;
    }

    public Message unwrap() {
        return message.unwrap();
    }

    public boolean isDurable() {
        return metadata.getAsBoolean(AmqpMetadata.DURABLE);
    }

    public long getDeliveryCount() {
        return metadata.getAsLong(AmqpMetadata.DELIVERY_COUNT, 0);
    }

    public int getPriority() {
        return metadata.getAsInteger(AmqpMetadata.PRIORITY, 0);

    }

    public long getTtl() {
        return metadata.getAsLong(AmqpMetadata.TTL, 0);

    }

    public Object getMessageId() {
        return metadata.get(AmqpMetadata.ID);
    }

    public long getGroupSequence() {
        return metadata.getAsLong(AmqpMetadata.GROUP_SEQUENCE, 0);
    }

    public long getCreationTime() {
        return metadata.getAsLong(AmqpMetadata.CREATION_TIME, 0);
    }

    public String getAddress() {
        return metadata.getAsString(AmqpMetadata.ADDRESS, null);
    }

    public String getGroupId() {
        return metadata.getAsString(AmqpMetadata.GROUP_ID, null);
    }

    public String getContentType() {
        return metadata.getAsString(AmqpMetadata.CONTENT_TYPE, null);
    }

    public long getExpiryTime() {
        return metadata.getAsLong(AmqpMetadata.EXPIRY_TIME, 0);
    }

    public Object getCorrelationId() {
        return metadata.get(AmqpMetadata.CORRELATION_ID);
    }

    public String getContentEncoding() {
        return metadata.getAsString(AmqpMetadata.CONTENT_ENCODING, null);
    }

    public String getSubject() {
        return metadata.getAsString(AmqpMetadata.SUBJECT, null);
    }

    public Header getHeader() {
        return metadata.get(AmqpMetadata.HEADER, new Header());
    }

    public JsonObject getApplicationProperties() {
        return metadata.get(AmqpMetadata.APPLICATION_PROPERTIES, new JsonObject());
    }

    public Section getBody() {
        return message.unwrap().getBody();
    }

    public MessageError getError() {
        return message.unwrap().getError();
    }

    public io.vertx.axle.amqp.AmqpMessage getAmqpMessage() {
        return new io.vertx.axle.amqp.AmqpMessage(message);
    }
}
