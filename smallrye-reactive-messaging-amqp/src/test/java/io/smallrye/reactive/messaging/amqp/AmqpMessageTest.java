package io.smallrye.reactive.messaging.amqp;

import static io.vertx.proton.ProtonHelper.message;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.message.MessageError;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.Test;

import io.vertx.amqp.impl.AmqpMessageImpl;
import io.vertx.core.json.JsonObject;

public class AmqpMessageTest {

    @Test
    public void testMessageAttributes() {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("hello", "world");
        props.put("some", "content");
        org.apache.qpid.proton.message.Message message = message();
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

        Message msg = AmqpMessageHelper.buildIncomingAmqpMessage(new AmqpMessageImpl(message));
        Optional iamd = msg.getMetadata(IncomingAmqpMetadata.class);
        IncomingAmqpMetadata incomingAmqpMetadata = (IncomingAmqpMetadata) iamd.get();
        MessageError messageError = (MessageError) msg.getMetadata(MessageError.class).get();
        assertThat(incomingAmqpMetadata.getAddress()).isEqualTo("address");
        assertThat(incomingAmqpMetadata.getProperties()).contains(entry("hello", "world"), entry("some", "content"));
        assertThat(incomingAmqpMetadata.getContentType()).isEqualTo("text/plain");
        assertThat(incomingAmqpMetadata.getCreationTime()).isNotZero();
        assertThat(incomingAmqpMetadata.getDeliveryCount()).isEqualTo(2);
        assertThat(incomingAmqpMetadata.getExpiryTime()).isEqualTo(10000);
        assertThat(incomingAmqpMetadata.getGroupId()).isEqualTo("some-group");
        assertThat(incomingAmqpMetadata.getTtl()).isEqualTo(1);
        assertThat(incomingAmqpMetadata.getSubject()).isEqualTo("subject");
        assertThat(incomingAmqpMetadata.getPriority()).isEqualTo((short) 2);
        assertThat(incomingAmqpMetadata.getCorrelationId()).isEqualTo("1234");
        assertThat(incomingAmqpMetadata.getId()).isEqualTo("4321");
        assertThat(incomingAmqpMetadata.getHeader()).isNotNull();
        assertThat(incomingAmqpMetadata.isDurable()).isTrue();
        assertThat(incomingAmqpMetadata.getGroupSequence()).isZero();

        assertThat(messageError.name()).isEqualTo("OK");

        assertThat(msg.getPayload()).isEqualTo("hello");

    }

    @Test
    public void testBuilder() {
        JsonObject json = new JsonObject();
        json.put("hello", "world");
        json.put("some", "content");

        OutgoingAmqpMetadata outgoingAmqpMetadata = OutgoingAmqpMetadata.builder().withSubject("subject")
            .withTtl(1)
            .withDurable(true)
            .withReplyTo("reply")
            .withProperties(json)
            .withContentType("text/plain")
            .withContentEncoding("utf-8")
            .withCorrelationId("1234")
            .withGroupId("some-group")
            .withAddress("address")
            .withPriority((short) 2)
            .withId("4321").build();

        Message<String> msg = org.eclipse.microprofile.reactive.messaging.Message.<String>newBuilder()
            .payload("hello").metadata(outgoingAmqpMetadata).build();

        OutgoingAmqpMetadata om = msg.getMetadata(OutgoingAmqpMetadata.class).get();
        assertThat(om.getAddress()).isEqualTo("address");
        assertThat(om.getReplyTo()).isEqualTo("reply");
        assertThat(om.getProperties()).contains(entry("hello", "world"), entry("some", "content"));
        assertThat(om.getContentType()).isEqualTo("text/plain");
        assertThat(om.getCreationTime()).isZero();
        assertThat(om.getDeliveryCount()).isZero();
        assertThat(om.getExpiryTime()).isZero();
        assertThat(om.getGroupId()).isEqualTo("some-group");
        assertThat(om.getTtl()).isEqualTo(1);
        assertThat(om.getSubject()).isEqualTo("subject");
        assertThat(om.getPriority()).isEqualTo((short) 2);
        assertThat(om.getCorrelationId()).isEqualTo("1234");
        assertThat(om.getId()).isEqualTo("4321");
        // Should this remain?  assertThat(om.getHeader()).isNotNull();
        assertThat(om.isDurable()).isTrue();
        // Should this remain? assertThat(messageError.name()).isEqualTo("OK");
        // Should this remain? assertThat(om.getGroupSequence()).isZero();

        assertThat(msg.getPayload()).isEqualTo("hello");
    }

}
