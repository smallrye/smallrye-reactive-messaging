package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.junit.jupiter.api.Test;

public class IncomingAmqpMetadataTest extends AmqpTestBase {

    @Test
    public void testIncomingAmqpMetadata() {
        String topic = UUID.randomUUID().toString();

        // Create an underlying proton message, as might be received
        org.apache.qpid.proton.message.Message protonMessage = Proton.message();
        protonMessage.setAddress(topic);
        protonMessage.setSubject("subject");
        protonMessage.setMessageId("my-id");
        protonMessage.setReplyTo("reply-to");
        protonMessage.setReplyToGroupId("reply-to-group");
        protonMessage.setPriority((short) 6);
        protonMessage.setTtl(2000);
        protonMessage.setGroupId("group");
        protonMessage.setContentType("text/plain");
        protonMessage.setCorrelationId("correlation-id");
        protonMessage.setUserId("user".getBytes(StandardCharsets.UTF_8));

        Map<Symbol, Object> daMap = new HashMap<>();
        daMap.put(Symbol.valueOf("some-delivery-annotation"), "da-value");
        DeliveryAnnotations da = new DeliveryAnnotations(daMap);
        protonMessage.setDeliveryAnnotations(da);

        Map<Symbol, Object> maMap = new HashMap<>();
        maMap.put(Symbol.valueOf("some-msg-annotation"), "ma-value");
        MessageAnnotations ma = new MessageAnnotations(maMap);
        protonMessage.setMessageAnnotations(ma);

        Map<String, Object> appPropsMap = new HashMap<>();
        appPropsMap.put("key", "value");
        ApplicationProperties appProps = new ApplicationProperties(appPropsMap);
        protonMessage.setApplicationProperties(appProps);

        Map<String, Object> footerMap = new HashMap<>();
        footerMap.put("my-trailer", "hello-footer");
        Footer footer = new Footer(footerMap);
        protonMessage.setFooter(footer);

        // Now test the metadata object accesses the metadata values as expected

        io.vertx.amqp.AmqpMessage vertxAmqpMessage = io.vertx.amqp.AmqpMessage.create(protonMessage).build();
        IncomingAmqpMetadata metadata = new IncomingAmqpMetadata(vertxAmqpMessage);

        assertThat(metadata.getAddress()).isEqualTo(topic);
        assertThat(metadata.getSubject()).isEqualTo("subject");
        assertThat(metadata.getId()).isEqualTo("my-id");
        assertThat(metadata.getReplyTo()).isEqualTo("reply-to");
        assertThat(metadata.getReplyToGroupId()).isEqualTo("reply-to-group");
        assertThat(metadata.getPriority()).isEqualTo((short) 6);
        assertThat(metadata.getTtl()).isEqualTo(2000);
        assertThat(metadata.getGroupId()).isEqualTo("group");
        assertThat(metadata.getContentType()).isEqualTo("text/plain");
        assertThat(metadata.getCorrelationId()).isEqualTo("correlation-id");
        assertThat(metadata.getUserId()).isEqualTo("user");
        assertThat(metadata.isFirstAcquirer()).isFalse();

        assertThat(metadata.getDeliveryAnnotations().getValue())
                .containsExactly(entry(Symbol.valueOf("some-delivery-annotation"), "da-value"));

        assertThat(metadata.getMessageAnnotations().getValue())
                .containsExactly(entry(Symbol.valueOf("some-msg-annotation"), "ma-value"));

        assertThat(metadata.getProperties()).containsExactly(entry("key", "value"));

        //noinspection unchecked
        assertThat(metadata.getFooter().getValue()).containsExactly(entry("my-trailer", "hello-footer"));
    }
}
