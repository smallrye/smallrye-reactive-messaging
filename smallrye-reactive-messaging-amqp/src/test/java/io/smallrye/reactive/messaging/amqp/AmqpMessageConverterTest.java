package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.vertx.amqp.impl.AmqpMessageImpl;

/**
 * The conversion of the outgoing messages from their payload and metadata.
 */
class AmqpMessageConverterTest {

    private static final Symbol ANNOTATION = Symbol.valueOf("x-opt-test");

    private static AmqpMessage<?> incoming(String replyTo) {
        org.apache.qpid.proton.message.Message proton = org.apache.qpid.proton.message.Message.Factory.create();
        proton.setAddress("incoming-address");
        proton.setReplyTo(replyTo);
        proton.setMessageId("id-1");
        proton.setUserId("incoming-user".getBytes(StandardCharsets.UTF_8));
        proton.setCorrelationId("corr-1");
        proton.setSubject("incoming-subject");
        proton.setContentType("text/plain");
        proton.setDurable(true);
        proton.setPriority((short) 7);
        proton.setTtl(3000);
        proton.setApplicationProperties(new ApplicationProperties(Map.of("kept", "incoming", "overridden", "incoming")));
        proton.setMessageAnnotations(new MessageAnnotations(Map.of(ANNOTATION, "incoming")));
        proton.setBody(new AmqpValue("incoming-body"));
        return new AmqpMessage<>(new AmqpMessageImpl(proton), null, null, false, false);
    }

    private static org.apache.qpid.proton.message.Message convert(Message<?> message) {
        return AmqpMessageConverter.convertToAmqpMessage(message, false, 0).getDelegate().unwrap();
    }

    @Test
    void doesNotCopyTheIncomingProperties() {
        org.apache.qpid.proton.message.Message output = convert(incoming(null).withPayload("changed"));

        assertThat(((AmqpValue) output.getBody()).getValue()).isEqualTo("changed");
        assertThat(output.getMessageId()).isNull();
        assertThat(output.getCorrelationId()).isNull();
        assertThat(output.getSubject()).isNull();
        assertThat(output.getContentType()).isNull();
        assertThat(output.getTtl()).isZero();
        assertThat(output.getApplicationProperties()).isNull();
        assertThat(output.getMessageAnnotations()).isNull();
        assertThat(output.getAddress()).isNull();
    }

    @Test
    void propagatesTheIncomingPropertiesCopiedByTheOutgoingMetadata() {
        AmqpMessage<?> in = incoming(null);
        Message<?> message = in.withPayload("changed")
                .addMetadata(OutgoingAmqpMetadata.from(in.getMetadata(IncomingAmqpMetadata.class).orElseThrow())
                        .withSubject("outgoing-subject")
                        .withApplicationProperty("overridden", "outgoing")
                        .build());

        org.apache.qpid.proton.message.Message output = convert(message);

        assertThat(((AmqpValue) output.getBody()).getValue()).isEqualTo("changed");
        assertThat(output.getMessageId()).isEqualTo("id-1");
        assertThat(output.getCorrelationId()).isEqualTo("corr-1");
        assertThat(output.getSubject()).isEqualTo("outgoing-subject");
        assertThat(output.getContentType()).isEqualTo("text/plain");
        assertThat(output.isDurable()).isTrue();
        assertThat(output.getPriority()).isEqualTo((short) 7);
        assertThat(output.getTtl()).isEqualTo(3000);
        assertThat(output.getApplicationProperties().getValue())
                .containsEntry("kept", "incoming")
                .containsEntry("overridden", "outgoing");
        assertThat(output.getMessageAnnotations().getValue()).containsEntry(ANNOTATION, "incoming");
        // Not copied: the address it was received on, and the user id, which brokers may check against the connection.
        assertThat(output.getAddress()).isNull();
        assertThat(output.getUserId()).isNull();
    }

    @Test
    void replyIsCorrelatedWithTheRequest() {
        org.apache.qpid.proton.message.Message output = convert(incoming("reply-address").withPayload("reply"));

        assertThat(output.getCorrelationId()).isEqualTo("id-1");
        // The reply-to addressed this reply: it is used to route it, not copied.
        assertThat(output.getReplyTo()).isNull();
    }
}
