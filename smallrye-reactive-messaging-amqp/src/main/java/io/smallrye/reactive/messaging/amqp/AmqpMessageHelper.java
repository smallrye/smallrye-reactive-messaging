package io.smallrye.reactive.messaging.amqp;

import io.vertx.axle.core.buffer.Buffer;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class AmqpMessageHelper {

    private AmqpMessageHelper() {
    }

    public static <T> Message<T> buildIncomingAmqpMessage(io.vertx.axle.amqp.AmqpMessage delegate) {
        return buildIncomingAmqpMessage(delegate.getDelegate());
    }

    public static <T> Message<T> buildIncomingAmqpMessage(io.vertx.amqp.AmqpMessage msg) {
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
        if (msg.unwrap().getHeader() != null) {
            builder.withHeader(msg.unwrap().getHeader());
        }


        return Message.<T>newBuilder()
            .payload(convert(msg)).metadata(builder.build(), msg.unwrap().getError()).ack(() -> {
                msg.accepted();
                return CompletableFuture.completedFuture(null);
            }).build();
    }


    private static <T> T convert(io.vertx.amqp.AmqpMessage msg) {
        Object body = msg.unwrap().getBody();
        if (body instanceof AmqpValue) {
            Object value = ((AmqpValue) body).getValue();
            if (value instanceof Binary) {
                Binary bin = (Binary) value;
                byte[] bytes = new byte[bin.getLength()];
                System.arraycopy(bin.getArray(), bin.getArrayOffset(), bytes, 0, bin.getLength());
                return (T) bytes;
            }
            return (T) value;
        }

        if (body instanceof AmqpSequence) {
            List list = ((AmqpSequence) body).getValue();
            return (T) list;
        }

        if (body instanceof Data) {
            Binary bin = ((Data) body).getValue();
            byte[] bytes = new byte[bin.getLength()];
            System.arraycopy(bin.getArray(), bin.getArrayOffset(), bytes, 0, bin.getLength());

            if ("application/json".equalsIgnoreCase(msg.contentType())) {
                return (T) Buffer.buffer(bytes).toJson();
            }
            return (T) bytes;
        }

        return (T) body;
    }

}
