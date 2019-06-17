package io.smallrye.reactive.messaging.amqp;

import static io.vertx.proton.ProtonHelper.message;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.MessageError;

import io.vertx.amqp.impl.AmqpMessageImpl;
import io.vertx.axle.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class AmqpMessage<T> implements org.eclipse.microprofile.reactive.messaging.Message<T> {

    private final io.vertx.amqp.AmqpMessage message;
    private final boolean received;

    public AmqpMessage(io.vertx.axle.amqp.AmqpMessage delegate) {
        this.message = delegate.getDelegate();
        this.received = true;
    }

    public AmqpMessage(T payload) {
        Message msg = message();
        if (payload instanceof Section) {
            msg.setBody((Section) payload);
        } else {
            msg.setBody(new AmqpValue(payload));
        }
        this.message = new AmqpMessageImpl(msg);
        this.received = false;
    }

    public AmqpMessage(io.vertx.amqp.AmqpMessage msg) {
        this.message = msg;
        this.received = false;
    }

    @Override
    public CompletionStage<Void> ack() {
        if (received) {
            this.message.accepted();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public T getPayload() {
        return (T) convert(message);
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
        return message.isDurable();
    }

    public long getDeliveryCount() {
        return message.deliveryCount();
    }

    public int getPriority() {
        return message.priority();
    }

    public long getTtl() {
        return message.ttl();
    }

    public Object getMessageId() {
        return message.id();
    }

    public long getGroupSequence() {
        return message.groupSequence();
    }

    public long getCreationTime() {
        return message.creationTime();
    }

    public String getAddress() {
        return message.address();
    }

    public String getGroupId() {
        return message.groupId();
    }

    public String getContentType() {
        return message.contentType();
    }

    public long getExpiryTime() {
        return message.expiryTime();
    }

    public Object getCorrelationId() {
        return message.correlationId();
    }

    public String getContentEncoding() {
        return message.contentEncoding();
    }

    public String getSubject() {
        return message.subject();
    }

    public Header getHeader() {
        return message.unwrap().getHeader();
    }

    public JsonObject getApplicationProperties() {
        return message.applicationProperties();
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
