package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Date;
import java.util.Optional;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;

import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQMessageConverter.OutgoingRabbitMQMessage;

public class RabbitMQMetadataTest {

    @Test
    void testIncomingMetadata() {
        Date timestamp = new Date();

        Envelope envelope = new Envelope(1, false, "test-exchange", "test-key");
        BasicProperties properties = new BasicProperties.Builder()
                .userId("test-user")
                .appId("tests")
                .contentType("text/plain")
                .contentEncoding("utf8")
                .correlationId("req-123")
                .deliveryMode(11)
                .expiration("1000")
                .priority(100)
                .messageId("12345")
                .replyTo("test-source")
                .timestamp(timestamp)
                .type("test-type")
                .build();
        byte[] body = new byte[] { 1, 2, 3, 4, 5 };

        IncomingRabbitMQMetadata incoming = new IncomingRabbitMQMetadata(envelope, properties, body, null);
        assertThat(incoming.getUserId()).isEqualTo("test-user");
        assertThat(incoming.getAppId()).isEqualTo("tests");
        assertThat(incoming.getContentType()).isEqualTo("text/plain");
        assertThat(incoming.getContentEncoding()).isEqualTo("utf8");
        assertThat(incoming.getCorrelationId()).isEqualTo("req-123");
        assertThat(incoming.getDeliveryMode()).isEqualTo(11);
        assertThat(incoming.getExpiration()).isEqualTo("1000");
        assertThat(incoming.getPriority()).isEqualTo(100);
        assertThat(incoming.getMessageId()).isEqualTo("12345");
        assertThat(incoming.getReplyTo()).isEqualTo("test-source");
        assertThat(incoming.getTimestamp()).isEqualTo(timestamp);
        assertThat(incoming.getType()).isEqualTo("test-type");
    }

    @Test
    void testOutgoingMetadata() {
        Date timestamp = new Date();

        OutgoingRabbitMQMetadata metadata = OutgoingRabbitMQMetadata.builder()
                .withUserId("test-user")
                .withAppId("tests")
                .withContentType("text/plain")
                .withContentEncoding("utf8")
                .withCorrelationId("req-123")
                .withDeliveryMode(11)
                .withExpiration("1000")
                .withPriority(100)
                .withMessageId("12345")
                .withReplyTo("test-source")
                .withTimestamp(timestamp)
                .withType("test-type")
                .build();

        OutgoingRabbitMQMessage message = RabbitMQMessageConverter.convert(
                Message.of("", Metadata.of(metadata)),
                "#",
                Optional.empty());

        com.rabbitmq.client.BasicProperties props = message.getProperties();

        assertThat(props.getUserId()).isEqualTo("test-user");
        assertThat(props.getAppId()).isEqualTo("tests");
        assertThat(props.getContentType()).isEqualTo("text/plain");
        assertThat(props.getContentEncoding()).isEqualTo("utf8");
        assertThat(props.getCorrelationId()).isEqualTo("req-123");
        assertThat(props.getDeliveryMode()).isEqualTo(11);
        assertThat(props.getExpiration()).isEqualTo("1000");
        assertThat(props.getPriority()).isEqualTo(100);
        assertThat(props.getMessageId()).isEqualTo("12345");
        assertThat(props.getReplyTo()).isEqualTo("test-source");
        assertThat(props.getTimestamp()).isEqualTo(timestamp);
        assertThat(props.getType()).isEqualTo("test-type");
    }

    // testOutgoingToIncomingMetadata is skipped:
    // og's OutgoingRabbitMQMetadata does not have a toIncomingMetadata() method

}
