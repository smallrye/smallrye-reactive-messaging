package io.smallrye.reactive.messaging.amqp;

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


  private final ProtonDelivery delivery;
  private final Message message;

  public AmqpMessage(ProtonDelivery delivery, Message message) {
    this.delivery = delivery;
    this.message = message;
  }

  public AmqpMessage(Message message) {
    this.message = message;
    this.delivery = null;
  }

  public AmqpMessage(T payload) {
    this.message = message();
    if (payload instanceof Section) {
      this.message.setBody((Section) payload);
    } else {
      this.message.setBody(new AmqpValue(payload));
    }
    this.delivery = null;
  }

  @Override
  public T getPayload() {
    return (T) convert((message.getBody()));
  }

  private Object convert(Object body) {
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
      return bytes;
    }

    return body;
  }

  @Override
  public CompletionStage<Void> ack() {
    if (delivery == null) {
      throw new IllegalStateException("Cannot acknowledge a message that is going to be sent");
    }
    // TODO is this really non-blocking?
    delivery.disposition(Accepted.getInstance(), true);
    return CompletableFuture.completedFuture(null);
  }

  public Message unwrap() {
    return message;
  }

  public ProtonDelivery delivery() {
    return delivery;
  }

  public boolean isDurable() {
    return message.isDurable();
  }

  public long getDeliveryCount() {
    return message.getDeliveryCount();
  }

  public short getPriority() {
    return message.getPriority();
  }

  public long getTtl() {
    return message.getTtl();
  }

  public Object getMessageId() {
    return message.getMessageId();
  }

  public long getGroupSequence() {
    return message.getGroupSequence();
  }

  public long getCreationTime() {
    return message.getCreationTime();
  }

  public String getAddress() {
    return message.getAddress();
  }

  public byte[] getUserId() {
    return message.getUserId();
  }

  public String getGroupId() {
    return message.getGroupId();
  }

  public String getContentType() {
    return message.getContentType();
  }

  public long getExpiryTime() {
    return message.getExpiryTime();
  }

  public Object getCorrelationId() {
    return message.getCorrelationId();
  }

  public String getContentEncoding() {
    return message.getContentEncoding();
  }

  public String getSubject() {
    return message.getSubject();
  }

  public Header getHeader() {
    return message.getHeader();
  }

  public DeliveryAnnotations getDeliveryAnnotations() {
    return message.getDeliveryAnnotations();
  }

  public MessageAnnotations getMessageAnnotations() {
    return message.getMessageAnnotations();
  }

  public Properties getProperties() {
    return message.getProperties();
  }

  public ApplicationProperties getApplicationProperties() {
    return message.getApplicationProperties();
  }

  public Section getBody() {
    return message.getBody();
  }

  public Footer getFooter() {
    return message.getFooter();
  }

  public MessageError getError() {
    return message.getError();
  }
}
