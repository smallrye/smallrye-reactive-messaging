package io.smallrye.reactive.messaging.amqp;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.MessageError;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;

public class AmqpMessage<T> implements org.eclipse.microprofile.reactive.messaging.Message<T> {

    protected final io.vertx.amqp.AmqpMessage message;
    protected final Metadata metadata;
    protected final IncomingAmqpMetadata amqpMetadata;

    public static <T> AmqpMessageBuilder<T> builder() {
        return new AmqpMessageBuilder<>();
    }

    public AmqpMessage(io.vertx.mutiny.amqp.AmqpMessage delegate) {
        this(delegate.getDelegate());
    }

    public AmqpMessage(io.vertx.amqp.AmqpMessage msg) {
        this.message = msg;
        IncomingAmqpMetadata.IncomingAmqpMetadataBuilder builder = new IncomingAmqpMetadata.IncomingAmqpMetadataBuilder();
        if (msg.address() != null) {
            builder.withAddress(msg.address());
        }
        if (msg.applicationProperties() != null) {
            builder.withProperties(msg.applicationProperties());
        }
        if (msg.contentType() != null) {
            builder.withContentType(msg.contentType());
        }
        if (msg.contentEncoding() != null) {
            builder.withContentEncoding(msg.contentEncoding());
        }
        if (msg.correlationId() != null) {
            builder.withCorrelationId(msg.correlationId());
        }
        if (msg.creationTime() > 0) {
            builder.withCreationTime(msg.creationTime());
        }
        if (msg.deliveryCount() >= 0) {
            builder.withDeliveryCount(msg.deliveryCount());
        }
        if (msg.expiryTime() >= 0) {
            builder.withExpirationTime(msg.expiryTime());
        }
        if (msg.groupId() != null) {
            builder.withGroupId(msg.groupId());
        }
        if (msg.groupSequence() >= 0) {
            builder.withGroupSequence(msg.groupSequence());
        }
        if (msg.id() != null) {
            builder.withId(msg.id());
        }
        builder.withDurable(msg.isDurable());
        builder.withFirstAcquirer(msg.isFirstAcquirer());
        if (msg.priority() >= 0) {
            builder.withPriority(msg.priority());
        }
        if (msg.subject() != null) {
            builder.withSubject(msg.subject());
        }
        if (msg.ttl() >= 0) {
            builder.withTtl(msg.ttl());
        }
        if (message.unwrap().getHeader() != null) {
            builder.withHeader(message.unwrap().getHeader());
        }
        this.amqpMetadata = builder.build();
        this.metadata = Metadata.of(builder.build());
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
        return amqpMetadata.isDurable();
    }

    public long getDeliveryCount() {
        return amqpMetadata.getDeliveryCount();
    }

    public int getPriority() {
        return amqpMetadata.getPriority();
    }

    public long getTtl() {
        return amqpMetadata.getTtl();
    }

    public Object getMessageId() {
        return amqpMetadata.getId();
    }

    public long getGroupSequence() {
        return amqpMetadata.getGroupSequence();
    }

    public long getCreationTime() {
        return amqpMetadata.getCreationTime();
    }

    public String getAddress() {
        return amqpMetadata.getAddress();
    }

    public String getGroupId() {
        return amqpMetadata.getGroupId();
    }

    public String getContentType() {
        return amqpMetadata.getContentType();
    }

    public long getExpiryTime() {
        return amqpMetadata.getExpiryTime();
    }

    public Object getCorrelationId() {
        return amqpMetadata.getCorrelationId();
    }

    public String getContentEncoding() {
        return amqpMetadata.getContentEncoding();
    }

    public String getSubject() {
        return amqpMetadata.getSubject();
    }

    public Header getHeader() {
        return amqpMetadata.getHeader();
    }

    public JsonObject getApplicationProperties() {
        return amqpMetadata.getProperties();
    }

    public Section getBody() {
        return message.unwrap().getBody();
    }

    public MessageError getError() {
        return message.unwrap().getError();
    }

    public io.vertx.mutiny.amqp.AmqpMessage getAmqpMessage() {
        return new io.vertx.mutiny.amqp.AmqpMessage(message);
    }
}
