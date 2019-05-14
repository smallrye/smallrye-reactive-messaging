package io.smallrye.reactive.messaging.amqp;

import io.vertx.ext.amqp.impl.AmqpMessageImpl;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.vertx.proton.ProtonHelper.message;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class AmqpMessageTest {


  @Test
  public void testMessageAttributes() {
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("hello", "world");
    props.put("some", "content");
    Message message = message();
    message.setTtl(1);
    message.setDurable(true);
    message.setReplyTo("reply");
    ApplicationProperties apps = new ApplicationProperties(props);
    message.setApplicationProperties(apps);
    message.setContentType("text/plain");
    message.setCorrelationId("1234");
    message.setDeliveryCount(2);
    message.setExpiryTime(10000);
    message.setFooter(new Footer(props));
    message.setGroupId("some-group");
    message.setAddress("address");
    message.setCreationTime(System.currentTimeMillis());
    message.setSubject("subject");
    message.setUserId("username".getBytes());
    message.setPriority((short) 2);
    message.setBody(new AmqpValue("hello"));
    message.setMessageId("4321");


    AmqpMessage msg = new AmqpMessage(new AmqpMessageImpl(message));
    assertThat(msg.getAddress()).isEqualTo("address");
    assertThat(msg.getApplicationProperties()).contains(entry("hello", "world"), entry("some", "content"));
    assertThat(msg.getContentType()).isEqualTo("text/plain");
    assertThat(msg.getCreationTime()).isNotZero();
    assertThat(msg.getDeliveryCount()).isEqualTo(2);
    assertThat(msg.getExpiryTime()).isEqualTo(10000);
    assertThat(msg.getGroupId()).isEqualTo("some-group");
    assertThat(msg.getTtl()).isEqualTo(1);
    assertThat(msg.getSubject()).isEqualTo("subject");
    assertThat(msg.getPriority()).isEqualTo((short) 2);
    assertThat(((AmqpValue)msg.getBody()).getValue()).isEqualTo("hello");
    assertThat(msg.getCorrelationId()).isEqualTo("1234");
    assertThat(msg.getMessageId()).isEqualTo("4321");
    assertThat(msg.getHeader()).isNotNull();
    assertThat(msg.isDurable()).isTrue();
    assertThat(msg.getError().name()).isEqualTo("OK");
    assertThat(msg.getGroupSequence()).isZero();

  }

}
