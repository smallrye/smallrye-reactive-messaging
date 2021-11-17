package io.smallrye.reactive.messaging.amqp;

import static io.vertx.proton.ProtonHelper.message;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.Test;

import io.vertx.amqp.impl.AmqpMessageImpl;
import io.vertx.core.json.JsonObject;

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
        message.setContentEncoding("encoding");
        message.setUserId("user".getBytes());
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
        message.setReplyTo("rep");
        message.setReplyToGroupId("rep-id");
        message.setBody(new AmqpValue("hello"));
        message.setMessageId("4321");

        AmqpMessage<?> msg = new AmqpMessage<>(new AmqpMessageImpl(message), null, null, false, false);
        assertThat(msg.getAddress()).isEqualTo("address");
        assertThat(msg.getApplicationProperties()).contains(entry("hello", "world"), entry("some", "content"));
        assertThat(msg.getContentType()).isEqualTo("text/plain");
        assertThat(msg.getContentEncoding()).isEqualTo("encoding");
        assertThat(msg.unwrap().getUserId()).isNotNull();
        assertThat(msg.unwrap().isFirstAcquirer()).isFalse();
        assertThat(msg.unwrap().getReplyTo()).isEqualTo("rep");
        assertThat(msg.unwrap().getReplyToGroupId()).isEqualTo("rep-id");
        assertThat(msg.getCreationTime()).isNotZero();
        assertThat(msg.getDeliveryCount()).isEqualTo(2);
        assertThat(msg.getExpiryTime()).isEqualTo(10000);
        assertThat(msg.getGroupId()).isEqualTo("some-group");
        assertThat(msg.getTtl()).isEqualTo(1);
        assertThat(msg.getSubject()).isEqualTo("subject");
        assertThat(msg.getPriority()).isEqualTo((short) 2);
        assertThat(((AmqpValue) msg.getBody()).getValue()).isEqualTo("hello");
        assertThat(msg.getCorrelationId()).isEqualTo("1234");
        assertThat(msg.getMessageId()).isEqualTo("4321");
        assertThat(msg.isDurable()).isTrue();
        assertThat(msg.getError().name()).isEqualTo("OK");
        assertThat(msg.getGroupSequence()).isZero();

    }

    @SuppressWarnings("deprecation")
    @Test
    public void testBuilder() {
        AmqpMessageBuilder<String> builder = AmqpMessage.builder();
        JsonObject json = new JsonObject();
        json.put("hello", "world");
        json.put("some", "content");

        builder.withSubject("subject")
                .withTtl(1)
                .withDurable(true)
                .withReplyTo("reply")
                .withApplicationProperties(json)
                .withContentType("text/plain")
                .withContentEncoding("utf-8")
                .withCorrelationId("1234")
                .withGroupId("some-group")
                .withAddress("address")
                .withPriority((short) 2)
                .withBody("hello")
                .withId("4321");

        AmqpMessage<String> msg = builder.build();
        assertThat(msg.getAddress()).isEqualTo("address");
        assertThat(msg.getApplicationProperties()).contains(entry("hello", "world"), entry("some", "content"));
        assertThat(msg.getContentType()).isEqualTo("text/plain");
        assertThat(msg.getCreationTime()).isZero();
        assertThat(msg.getDeliveryCount()).isZero();
        assertThat(msg.getExpiryTime()).isZero();
        assertThat(msg.getGroupId()).isEqualTo("some-group");
        assertThat(msg.getTtl()).isEqualTo(1);
        assertThat(msg.getSubject()).isEqualTo("subject");
        assertThat(msg.getPriority()).isEqualTo((short) 2);
        assertThat(((AmqpValue) msg.getBody()).getValue()).isEqualTo("hello");
        assertThat(msg.getCorrelationId()).isEqualTo("1234");
        assertThat(msg.getMessageId()).isEqualTo("4321");
        assertThat(msg.isDurable()).isTrue();
        assertThat(msg.getError().name()).isEqualTo("OK");
        assertThat(msg.getGroupSequence()).isZero();

        assertThat(AmqpMessage.<Boolean> builder().withBooleanAsBody(true).build().getPayload().booleanValue()).isTrue();
        assertThat(AmqpMessage.<Integer> builder().withIntegerAsBody(23).build().getPayload()).isEqualTo(23);
        assertThat(AmqpMessage.<Long> builder().withLongAsBody(23L).build().getPayload()).isEqualTo(23L);
    }

}
