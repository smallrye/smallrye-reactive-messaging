package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAckHandler;
import io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNackHandler;

public class IncomingRabbitMQMessageTest {

    private static final AMQP.BasicProperties EMPTY_PROPS = new AMQP.BasicProperties.Builder().build();

    RabbitMQAckHandler doNothingAck = new RabbitMQAckHandler() {
        @Override
        public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message) {
            return CompletableFuture.completedFuture(null);
        }
    };

    RabbitMQNackHandler doNothingNack = new RabbitMQNackHandler() {
        @Override
        public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message, Metadata metadata,
                Throwable reason) {
            return CompletableFuture.completedFuture(null);
        }
    };

    @Test
    public void testDoubleAckBehavior() {
        Envelope envelope = new Envelope(13456L, false, "test", "test");

        Exception nackReason = new Exception("test");

        IncomingRabbitMQMessage<String> ackMsg = new IncomingRabbitMQMessage<>(envelope,
                EMPTY_PROPS, new byte[0],
                IncomingRabbitMQMessage.STRING_CONVERTER,
                doNothingAck, doNothingNack,
                null, "text/plain");

        assertDoesNotThrow(() -> ackMsg.ack().toCompletableFuture().get());
        assertDoesNotThrow(() -> ackMsg.ack().toCompletableFuture().get());
        assertDoesNotThrow(() -> ackMsg.nack(nackReason).toCompletableFuture().get());
    }

    @Test
    public void testDoubleNackBehavior() {
        Envelope envelope = new Envelope(13456L, false, "test", "test");

        Exception nackReason = new Exception("test");

        IncomingRabbitMQMessage<String> nackMsg = new IncomingRabbitMQMessage<>(envelope,
                EMPTY_PROPS, new byte[0],
                IncomingRabbitMQMessage.STRING_CONVERTER,
                doNothingAck, doNothingNack,
                null, "text/plain");

        assertDoesNotThrow(() -> nackMsg.nack(nackReason).toCompletableFuture().get());
        assertDoesNotThrow(() -> nackMsg.nack(nackReason).toCompletableFuture().get());
        assertDoesNotThrow(() -> nackMsg.ack().toCompletableFuture().get());
    }

    // --- getEffectiveContentType tests ---

    @Test
    void testEffectiveContentTypeWithOverride() {
        Envelope envelope = new Envelope(1L, false, "test", "test");
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("application/json").build();

        IncomingRabbitMQMessage<String> incoming = new IncomingRabbitMQMessage<>(envelope,
                properties, new byte[0],
                IncomingRabbitMQMessage.STRING_CONVERTER,
                doNothingAck, doNothingNack,
                null, "text/plain");

        // Override takes precedence over the property
        assertThat(incoming.getRabbitMQMetadata().getEffectiveContentType()).hasValue("text/plain");
        // getContentType returns the raw property value
        assertThat(incoming.getRabbitMQMetadata().getContentType()).isEqualTo("application/json");
    }

    @Test
    void testEffectiveContentTypeWithNullOverride() {
        Envelope envelope = new Envelope(1L, false, "test", "test");
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("application/xml").build();

        IncomingRabbitMQMessage<String> incoming = new IncomingRabbitMQMessage<>(envelope,
                properties, new byte[0],
                IncomingRabbitMQMessage.STRING_CONVERTER,
                doNothingAck, doNothingNack,
                null, null);

        // No override -> falls back to property
        assertThat(incoming.getRabbitMQMetadata().getEffectiveContentType()).hasValue("application/xml");
    }

    @Test
    void testEffectiveContentTypeWithNullOverrideAndNullProperty() {
        Envelope envelope = new Envelope(1L, false, "test", "test");

        IncomingRabbitMQMessage<String> incoming = new IncomingRabbitMQMessage<>(envelope,
                EMPTY_PROPS, new byte[0],
                IncomingRabbitMQMessage.STRING_CONVERTER,
                doNothingAck, doNothingNack,
                null, null);

        assertThat(incoming.getRabbitMQMetadata().getEffectiveContentType()).isEmpty();
    }

    // --- Content encoding warning path ---

    @Test
    void testConstructorWithContentEncodingAndNonOctetStreamContentType() {
        Envelope envelope = new Envelope(1L, false, "test", "test");
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("text/plain")
                .contentEncoding("UTF-8")
                .build();

        // Should not throw -- the warning is logged but creation succeeds
        IncomingRabbitMQMessage<String> incoming = new IncomingRabbitMQMessage<>(envelope,
                properties, "data".getBytes(),
                IncomingRabbitMQMessage.STRING_CONVERTER,
                doNothingAck, doNothingNack,
                null, null);

        assertThat(incoming.getRabbitMQMetadata().getContentEncoding()).isEqualTo("UTF-8");
        assertThat(incoming.getRabbitMQMetadata().getContentType()).isEqualTo("text/plain");
    }

    @Test
    void testConstructorWithContentEncodingAndOctetStreamContentType() {
        Envelope envelope = new Envelope(1L, false, "test", "test");
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("application/octet-stream")
                .contentEncoding("binary")
                .build();

        // Binary content with encoding -- no warning should be logged
        IncomingRabbitMQMessage<byte[]> incoming = new IncomingRabbitMQMessage<>(envelope,
                properties, new byte[] { 0x01, 0x02 },
                IncomingRabbitMQMessage.BYTE_ARRAY_CONVERTER,
                doNothingAck, doNothingNack,
                null, null);

        assertThat(incoming.getRabbitMQMetadata().getContentEncoding()).isEqualTo("binary");
    }

    @Test
    void testConstructorWithNullContentEncoding() {
        Envelope envelope = new Envelope(1L, false, "test", "test");
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("text/plain")
                .build();

        IncomingRabbitMQMessage<String> incoming = new IncomingRabbitMQMessage<>(envelope,
                properties, "data".getBytes(),
                IncomingRabbitMQMessage.STRING_CONVERTER,
                doNothingAck, doNothingNack,
                null, null);

        assertThat(incoming.getRabbitMQMetadata().getContentEncoding()).isNull();
    }

    // --- injectMetadata ---

    @Test
    void testInjectMetadata() {
        Envelope envelope = new Envelope(1L, false, "test", "test");

        IncomingRabbitMQMessage<String> incoming = new IncomingRabbitMQMessage<>(envelope,
                EMPTY_PROPS, new byte[0],
                IncomingRabbitMQMessage.STRING_CONVERTER,
                doNothingAck, doNothingNack,
                null, null);

        // Metadata should contain IncomingRabbitMQMetadata by default
        assertThat(incoming.getMetadata(IncomingRabbitMQMetadata.class)).isPresent();

        // Inject custom metadata
        String customMeta = "custom-metadata";
        incoming.injectMetadata(customMeta);

        // Both original and injected metadata should be accessible
        assertThat(incoming.getMetadata(IncomingRabbitMQMetadata.class)).isPresent();
        assertThat(incoming.getMetadata(String.class)).hasValue(customMeta);
    }

    // --- Metadata delegation methods ---

    @Test
    void testMetadataDelegation() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("x-custom", "header-value");

        Date timestamp = new Date();
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("text/plain")
                .contentEncoding("UTF-8")
                .headers(headers)
                .deliveryMode(2)
                .priority(5)
                .correlationId("corr-123")
                .replyTo("reply-queue")
                .expiration("60000")
                .messageId("msg-001")
                .timestamp(timestamp)
                .type("test-type")
                .userId("user1")
                .appId("app1")
                .build();
        Envelope envelope = new Envelope(42L, true, "exchange1", "rk1");

        IncomingRabbitMQMessage<String> incoming = new IncomingRabbitMQMessage<>(envelope,
                properties, "body".getBytes(),
                IncomingRabbitMQMessage.STRING_CONVERTER,
                doNothingAck, doNothingNack,
                null, null);

        IncomingRabbitMQMetadata metadata = incoming.getRabbitMQMetadata();
        assertThat(metadata.getHeaders()).containsEntry("x-custom", "header-value");
        assertThat(metadata.getContentType()).isEqualTo("text/plain");
        assertThat(metadata.getContentEncoding()).isEqualTo("UTF-8");
        assertThat(metadata.getDeliveryMode()).isEqualTo(2);
        assertThat(metadata.getPriority()).isEqualTo(5);
        assertThat(metadata.getCorrelationId()).isEqualTo("corr-123");
        assertThat(metadata.getReplyTo()).isEqualTo("reply-queue");
        assertThat(metadata.getExpiration()).isEqualTo("60000");
        assertThat(metadata.getMessageId()).isEqualTo("msg-001");
        assertThat(metadata.getTimestamp()).isEqualTo(timestamp);
        assertThat(metadata.getType()).isEqualTo("test-type");
        assertThat(metadata.getUserId()).isEqualTo("user1");
        assertThat(metadata.getAppId()).isEqualTo("app1");
    }

    @Test
    void testMetadataDelegationWithEmptyProperties() {
        Envelope envelope = new Envelope(1L, false, "test", "test");

        IncomingRabbitMQMessage<String> incoming = new IncomingRabbitMQMessage<>(envelope,
                EMPTY_PROPS, new byte[0],
                IncomingRabbitMQMessage.STRING_CONVERTER,
                doNothingAck, doNothingNack,
                null, null);

        IncomingRabbitMQMetadata metadata = incoming.getRabbitMQMetadata();
        assertThat(metadata.getContentType()).isNull();
        assertThat(metadata.getContentEncoding()).isNull();
        assertThat(metadata.getDeliveryMode()).isNull();
        assertThat(metadata.getPriority()).isNull();
        assertThat(metadata.getCorrelationId()).isNull();
        assertThat(metadata.getReplyTo()).isNull();
        assertThat(metadata.getExpiration()).isNull();
        assertThat(metadata.getMessageId()).isNull();
        assertThat(metadata.getTimestamp()).isNull();
        assertThat(metadata.getType()).isNull();
        assertThat(metadata.getUserId()).isNull();
        assertThat(metadata.getAppId()).isNull();
    }
}
