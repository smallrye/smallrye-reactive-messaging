package io.smallrye.reactive.messaging.amqp;

import io.vertx.axle.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.amqp.impl.AmqpMessageImpl;
import io.vertx.proton.ProtonDelivery;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.MessageError;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.vertx.proton.ProtonHelper.message;

public class AmqpMessage<T> implements org.eclipse.microprofile.reactive.messaging.Message<T> {

  private final io.vertx.amqp.AmqpMessage message;

  public AmqpMessage(io.vertx.axle.amqp.AmqpMessage delegate) {
    this.message = delegate.getDelegate();
  }

  public AmqpMessage(T payload) {
    Message msg = message();
    if (payload instanceof Section) {
      msg.setBody((Section) payload);
    } else {
      msg.setBody(new AmqpValue(payload));
    }
    this.message = new AmqpMessageImpl(msg);
  }

  public AmqpMessage(io.vertx.amqp.AmqpMessage msg) {
    this.message = msg;
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

  @Override
  public CompletionStage<Void> ack() {
    ((AmqpMessageImpl) message).delivered();
    return CompletableFuture.completedFuture(null);
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
