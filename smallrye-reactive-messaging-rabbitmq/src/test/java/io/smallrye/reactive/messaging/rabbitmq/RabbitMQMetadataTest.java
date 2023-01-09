package io.smallrye.reactive.messaging.rabbitmq;

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQMessageConverter.OutgoingRabbitMQMessage;
import io.vertx.core.buffer.Buffer;
import io.vertx.rabbitmq.RabbitMQMessage;

public class RabbitMQMetadataTest {

    @Test
    void testIncomingMetadata() {
        ZonedDateTime timestamp = ZonedDateTime.now().truncatedTo(MILLIS);

        RabbitMQMessage message = new RabbitMQMessage() {
            @Override
            public Buffer body() {
                return Buffer.buffer(new byte[] { 1, 2, 3, 4, 5 });
            }

            @Override
            public String consumerTag() {
                return "123";
            }

            @Override
            public Envelope envelope() {
                return new Envelope(1, false, "test-exchange", "test-key");
            }

            @Override
            public com.rabbitmq.client.BasicProperties properties() {
                return new BasicProperties.Builder()
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
                        .timestamp(Date.from(timestamp.toInstant()))
                        .type("test-type")
                        .build();
            }

            @Override
            public Integer messageCount() {
                return 5;
            }
        };

        IncomingRabbitMQMetadata incoming = new IncomingRabbitMQMetadata(message);
        assertThat(incoming.getUserId()).isEqualTo(Optional.of("test-user"));
        assertThat(incoming.getAppId()).isEqualTo(Optional.of("tests"));
        assertThat(incoming.getContentType()).isEqualTo(Optional.of("text/plain"));
        assertThat(incoming.getContentEncoding()).isEqualTo(Optional.of("utf8"));
        assertThat(incoming.getCorrelationId()).isEqualTo(Optional.of("req-123"));
        assertThat(incoming.getDeliveryMode()).isEqualTo(Optional.of(11));
        assertThat(incoming.getExpiration()).isEqualTo(Optional.of("1000"));
        assertThat(incoming.getPriority()).isEqualTo(Optional.of(100));
        assertThat(incoming.getMessageId()).isEqualTo(Optional.of("12345"));
        assertThat(incoming.getReplyTo()).isEqualTo(Optional.of("test-source"));
        assertThat(incoming.getTimestamp(timestamp.getZone())).isEqualTo(Optional.of(timestamp));
        assertThat(incoming.getType()).isEqualTo(Optional.of("test-type"));
    }

    @Test
    void testOutgoingMetadata() {
        ZonedDateTime timestamp = ZonedDateTime.now().truncatedTo(MILLIS);

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
                (Instrumenter) Instrumenter.builder(OpenTelemetry.noop(), "noop", o -> "noop").buildInstrumenter(),
                Message.of("", Metadata.of(metadata)),
                "test",
                "#",
                Optional.empty(),
                false);

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
        assertThat(props.getTimestamp()).isEqualTo(Date.from(timestamp.toInstant()));
        assertThat(props.getType()).isEqualTo("test-type");
    }

}
