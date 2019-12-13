package io.smallrye.reactive.messaging.amqp;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.MessageError;
import org.eclipse.microprofile.reactive.messaging.Headers;

import io.vertx.axle.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class AmqpMessage<T> implements org.eclipse.microprofile.reactive.messaging.Message<T> {

    protected final io.vertx.amqp.AmqpMessage message;
    protected final Headers headers;

    public static <T> AmqpMessageBuilder<T> builder() {
        return new AmqpMessageBuilder<>();
    }

    public AmqpMessage(io.vertx.axle.amqp.AmqpMessage delegate) {
        this.message = delegate.getDelegate();
        Headers.HeadersBuilder builder = Headers.builder();

        if (delegate.address() != null) {
            builder.with(AmqpHeaders.ADDRESS, delegate.address());
        }
        if (delegate.applicationProperties() != null) {
            builder.with(AmqpHeaders.APPLICATION_PROPERTIES, delegate.applicationProperties());
        }
        if (delegate.contentType() != null) {
            builder.with(AmqpHeaders.CONTENT_TYPE, delegate.contentType());
        }
        if (delegate.contentEncoding() != null) {
            builder.with(AmqpHeaders.CONTENT_ENCODING, delegate.contentEncoding());
        }
        if (delegate.correlationId() != null) {
            builder.with(AmqpHeaders.CORRELATION_ID, delegate.correlationId());
        }
        if (delegate.creationTime() > 0) {
            builder.with(AmqpHeaders.CREATION_TIME, delegate.creationTime());
        }
        if (delegate.deliveryCount() >= 0) {
            builder.with(AmqpHeaders.DELIVERY_COUNT, delegate.deliveryCount());
        }
        if (delegate.expiryTime() >= 0) {
            builder.with(AmqpHeaders.EXPIRY_TIME, delegate.expiryTime());
        }
        if (delegate.groupId() != null) {
            builder.with(AmqpHeaders.GROUP_ID, delegate.groupId());
        }
        if (delegate.groupSequence() >= 0) {
            builder.with(AmqpHeaders.GROUP_SEQUENCE, delegate.groupSequence());
        }
        if (delegate.id() != null) {
            builder.with(AmqpHeaders.ID, delegate.id());
        }
        builder.with(AmqpHeaders.DURABLE, delegate.isDurable());
        builder.with(AmqpHeaders.FIRST_ACQUIRER, delegate.isFirstAcquirer());
        if (delegate.priority() >= 0) {
            builder.with(AmqpHeaders.PRIORITY, delegate.priority());
        }
        if (delegate.subject() != null) {
            builder.with(AmqpHeaders.SUBJECT, delegate.subject());
        }
        if (delegate.ttl() >= 0) {
            builder.with(AmqpHeaders.TTL, delegate.ttl());
        }
        if (message.unwrap().getHeader() != null) {
            builder.with(AmqpHeaders.HEADER, message.unwrap().getHeader());
        }
        this.headers = builder.build();
    }

    public AmqpMessage(io.vertx.amqp.AmqpMessage msg) {
        this.message = msg;
        Headers.HeadersBuilder builder = Headers.builder();

        if (msg.address() != null) {
            builder.with(AmqpHeaders.ADDRESS, msg.address());
        }
        if (msg.applicationProperties() != null) {
            builder.with(AmqpHeaders.APPLICATION_PROPERTIES, msg.applicationProperties());
        }
        if (msg.contentType() != null) {
            builder.with(AmqpHeaders.CONTENT_TYPE, msg.contentType());
        }
        if (msg.contentEncoding() != null) {
            builder.with(AmqpHeaders.CONTENT_ENCODING, msg.contentEncoding());
        }
        if (msg.correlationId() != null) {
            builder.with(AmqpHeaders.CORRELATION_ID, msg.correlationId());
        }
        if (msg.creationTime() > 0) {
            builder.with(AmqpHeaders.CREATION_TIME, msg.creationTime());
        }
        if (msg.deliveryCount() >= 0) {
            builder.with(AmqpHeaders.DELIVERY_COUNT, msg.deliveryCount());
        }
        if (msg.expiryTime() >= 0) {
            builder.with(AmqpHeaders.EXPIRY_TIME, msg.expiryTime());
        }
        if (msg.groupId() != null) {
            builder.with(AmqpHeaders.GROUP_ID, msg.groupId());
        }
        if (msg.groupSequence() >= 0) {
            builder.with(AmqpHeaders.GROUP_SEQUENCE, msg.groupSequence());
        }
        if (msg.id() != null) {
            builder.with(AmqpHeaders.ID, msg.id());
        }
        builder.with(AmqpHeaders.DURABLE, msg.isDurable());
        builder.with(AmqpHeaders.FIRST_ACQUIRER, msg.isFirstAcquirer());
        if (msg.priority() >= 0) {
            builder.with(AmqpHeaders.PRIORITY, msg.priority());
        }
        if (msg.subject() != null) {
            builder.with(AmqpHeaders.SUBJECT, msg.subject());
        }
        if (msg.ttl() >= 0) {
            builder.with(AmqpHeaders.TTL, msg.ttl());
        }
        if (message.unwrap().getHeader() != null) {
            builder.with(AmqpHeaders.HEADER, message.unwrap().getHeader());
        }
        this.headers = builder.build();
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
    public Headers getHeaders() {
        return headers;
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
        return headers.getAsBoolean(AmqpHeaders.DURABLE);
    }

    public long getDeliveryCount() {
        return headers.getAsLong(AmqpHeaders.DELIVERY_COUNT, 0);
    }

    public int getPriority() {
        return headers.getAsInteger(AmqpHeaders.PRIORITY, 0);

    }

    public long getTtl() {
        return headers.getAsLong(AmqpHeaders.TTL, 0);

    }

    public Object getMessageId() {
        return headers.get(AmqpHeaders.ID);
    }

    public long getGroupSequence() {
        return headers.getAsLong(AmqpHeaders.GROUP_SEQUENCE, 0);
    }

    public long getCreationTime() {
        return headers.getAsLong(AmqpHeaders.CREATION_TIME, 0);
    }

    public String getAddress() {
        return headers.getAsString(AmqpHeaders.ADDRESS, null);
    }

    public String getGroupId() {
        return headers.getAsString(AmqpHeaders.GROUP_ID, null);
    }

    public String getContentType() {
        return headers.getAsString(AmqpHeaders.CONTENT_TYPE, null);
    }

    public long getExpiryTime() {
        return headers.getAsLong(AmqpHeaders.EXPIRY_TIME, 0);
    }

    public Object getCorrelationId() {
        return headers.get(AmqpHeaders.CORRELATION_ID);
    }

    public String getContentEncoding() {
        return headers.getAsString(AmqpHeaders.CONTENT_ENCODING, null);
    }

    public String getSubject() {
        return headers.getAsString(AmqpHeaders.SUBJECT, null);
    }

    public Header getHeader() {
        return headers.get(AmqpHeaders.HEADER, new Header());
    }

    public JsonObject getApplicationProperties() {
        return headers.get(AmqpHeaders.APPLICATION_PROPERTIES, new JsonObject());
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
