package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP.BasicProperties;

import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAckHandler;
import io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQFailureHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.mutiny.core.Context;

public class IncomingRabbitMQMessageTest {

    RabbitMQAckHandler doNothingAck = new RabbitMQAckHandler() {
        @Override
        public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message, Context context) {
            return CompletableFuture.completedFuture(null);
        }
    };

    RabbitMQFailureHandler doNothingNack = new RabbitMQFailureHandler() {
        @Override
        public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message, Metadata metadata, Context context,
                Throwable reason) {
            return CompletableFuture.completedFuture(null);
        }
    };

    @Test
    public void testDoubleAckBehavior() {
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .envelope(13456L, false, "test", "test")
                .build();

        Exception nackReason = new Exception("test");

        IncomingRabbitMQMessage<String> ackMsg = new IncomingRabbitMQMessage<>(testMsg, null, null,
                doNothingNack,
                doNothingAck,
                "text/plain");

        assertDoesNotThrow(() -> ackMsg.ack().toCompletableFuture().get());
        assertDoesNotThrow(() -> ackMsg.ack().toCompletableFuture().get());
        assertDoesNotThrow(() -> ackMsg.nack(nackReason).toCompletableFuture().get());
    }

    @Test
    public void testDoubleNackBehavior() {
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .envelope(13456L, false, "test", "test")
                .build();

        Exception nackReason = new Exception("test");

        IncomingRabbitMQMessage<String> nackMsg = new IncomingRabbitMQMessage<>(testMsg, null, null,
                doNothingNack,
                doNothingAck,
                "text/plain");

        assertDoesNotThrow(() -> nackMsg.nack(nackReason).toCompletableFuture().get());
        assertDoesNotThrow(() -> nackMsg.nack(nackReason).toCompletableFuture().get());
        assertDoesNotThrow(() -> nackMsg.ack().toCompletableFuture().get());
    }

    @Test
    void testConvertPayloadFallback() {
        Buffer payloadBuffer = Buffer.buffer("payload");
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .body(payloadBuffer)
                .properties(new BasicProperties.Builder().contentType("application/json").build())
                .envelope(13456L, false, "test", "test")
                .build();

        IncomingRabbitMQMessage<Buffer> incomingRabbitMQMessage = new IncomingRabbitMQMessage<>(testMsg,
                null, null,
                doNothingNack, doNothingAck, null);

        assertThat(incomingRabbitMQMessage.getPayload()).isEqualTo(payloadBuffer);
    }

    // --- getEffectiveContentType tests ---

    @Test
    void testEffectiveContentTypeWithOverride() {
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .properties(new BasicProperties.Builder().contentType("application/json").build())
                .envelope(1L, false, "test", "test")
                .build();

        IncomingRabbitMQMessage<Buffer> incoming = new IncomingRabbitMQMessage<>(testMsg,
                null, null, doNothingNack, doNothingAck, "text/plain");

        // Override takes precedence over the property
        assertThat(incoming.getEffectiveContentType()).hasValue("text/plain");
        // getContentType returns the raw property value
        assertThat(incoming.getContentType()).hasValue("application/json");
    }

    @Test
    void testEffectiveContentTypeWithNullOverride() {
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .properties(new BasicProperties.Builder().contentType("application/xml").build())
                .envelope(1L, false, "test", "test")
                .build();

        IncomingRabbitMQMessage<Buffer> incoming = new IncomingRabbitMQMessage<>(testMsg,
                null, null, doNothingNack, doNothingAck, null);

        // No override → falls back to property
        assertThat(incoming.getEffectiveContentType()).hasValue("application/xml");
    }

    @Test
    void testEffectiveContentTypeWithNullOverrideAndNullProperty() {
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .envelope(1L, false, "test", "test")
                .build();

        IncomingRabbitMQMessage<Buffer> incoming = new IncomingRabbitMQMessage<>(testMsg,
                null, null, doNothingNack, doNothingAck, null);

        assertThat(incoming.getEffectiveContentType()).isEmpty();
    }

    // --- Content encoding warning path ---

    @Test
    void testConstructorWithContentEncodingAndNonOctetStreamContentType() {
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .body("data")
                .properties(new BasicProperties.Builder()
                        .contentType("text/plain")
                        .contentEncoding("UTF-8")
                        .build())
                .envelope(1L, false, "test", "test")
                .build();

        // Should not throw — the warning is logged but creation succeeds
        IncomingRabbitMQMessage<Buffer> incoming = new IncomingRabbitMQMessage<>(testMsg,
                null, null, doNothingNack, doNothingAck, null);

        assertThat(incoming.getContentEncoding()).hasValue("UTF-8");
        assertThat(incoming.getContentType()).hasValue("text/plain");
    }

    @Test
    void testConstructorWithContentEncodingAndOctetStreamContentType() {
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .body(new byte[] { 0x01, 0x02 })
                .properties(new BasicProperties.Builder()
                        .contentType("application/octet-stream")
                        .contentEncoding("binary")
                        .build())
                .envelope(1L, false, "test", "test")
                .build();

        // Binary content with encoding — no warning should be logged
        IncomingRabbitMQMessage<Buffer> incoming = new IncomingRabbitMQMessage<>(testMsg,
                null, null, doNothingNack, doNothingAck, null);

        assertThat(incoming.getContentEncoding()).hasValue("binary");
    }

    @Test
    void testConstructorWithNullContentEncoding() {
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .body("data")
                .properties(new BasicProperties.Builder()
                        .contentType("text/plain")
                        .build())
                .envelope(1L, false, "test", "test")
                .build();

        IncomingRabbitMQMessage<Buffer> incoming = new IncomingRabbitMQMessage<>(testMsg,
                null, null, doNothingNack, doNothingAck, null);

        assertThat(incoming.getContentEncoding()).isEmpty();
    }

    // --- injectMetadata ---

    @Test
    void testInjectMetadata() {
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .envelope(1L, false, "test", "test")
                .build();

        IncomingRabbitMQMessage<Buffer> incoming = new IncomingRabbitMQMessage<>(testMsg,
                null, null, doNothingNack, doNothingAck, null);

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
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .body("body")
                .properties(new BasicProperties.Builder()
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
                        .build())
                .envelope(42L, true, "exchange1", "rk1")
                .build();

        IncomingRabbitMQMessage<Buffer> incoming = new IncomingRabbitMQMessage<>(testMsg,
                null, null, doNothingNack, doNothingAck, null);

        assertThat(incoming.getHeaders()).containsEntry("x-custom", "header-value");
        assertThat(incoming.getContentType()).hasValue("text/plain");
        assertThat(incoming.getContentEncoding()).hasValue("UTF-8");
        assertThat(incoming.getDeliveryMode()).hasValue(2);
        assertThat(incoming.getPriority()).hasValue(5);
        assertThat(incoming.getCorrelationId()).hasValue("corr-123");
        assertThat(incoming.getReplyTo()).hasValue("reply-queue");
        assertThat(incoming.getExpiration()).hasValue("60000");
        assertThat(incoming.getMessageId()).hasValue("msg-001");
        assertThat(incoming.getTimestamp(ZoneOffset.UTC)).isPresent();
        assertThat(incoming.getType()).hasValue("test-type");
        assertThat(incoming.getUserId()).hasValue("user1");
        assertThat(incoming.getAppId()).hasValue("app1");
    }

    @Test
    void testMetadataDelegationWithEmptyProperties() {
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .envelope(1L, false, "test", "test")
                .build();

        IncomingRabbitMQMessage<Buffer> incoming = new IncomingRabbitMQMessage<>(testMsg,
                null, null, doNothingNack, doNothingAck, null);

        assertThat(incoming.getContentType()).isEmpty();
        assertThat(incoming.getContentEncoding()).isEmpty();
        assertThat(incoming.getDeliveryMode()).isEmpty();
        assertThat(incoming.getPriority()).isEmpty();
        assertThat(incoming.getCorrelationId()).isEmpty();
        assertThat(incoming.getReplyTo()).isEmpty();
        assertThat(incoming.getExpiration()).isEmpty();
        assertThat(incoming.getMessageId()).isEmpty();
        assertThat(incoming.getTimestamp(ZoneOffset.UTC)).isEmpty();
        assertThat(incoming.getType()).isEmpty();
        assertThat(incoming.getUserId()).isEmpty();
        assertThat(incoming.getAppId()).isEmpty();
    }

    @Test
    void testGetRabbitMQMessage() {
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .envelope(1L, false, "exchange", "routing-key")
                .build();

        IncomingRabbitMQMessage<Buffer> incoming = new IncomingRabbitMQMessage<>(testMsg,
                null, null, doNothingNack, doNothingAck, null);

        io.vertx.mutiny.rabbitmq.RabbitMQMessage retrieved = incoming.getRabbitMQMessage();
        assertThat(retrieved).isNotNull();
        assertThat(retrieved.envelope().getRoutingKey()).isEqualTo("routing-key");
        assertThat(retrieved.envelope().getExchange()).isEqualTo("exchange");
    }

    @SuppressWarnings("deprecation")
    @Test
    void testDeprecatedGetCreationTime() {
        Date timestamp = new Date();
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .properties(new BasicProperties.Builder().timestamp(timestamp).build())
                .envelope(1L, false, "test", "test")
                .build();

        IncomingRabbitMQMessage<Buffer> incoming = new IncomingRabbitMQMessage<>(testMsg,
                null, null, doNothingNack, doNothingAck, null);

        // getCreationTime is deprecated and delegates to getTimestamp
        assertThat(incoming.getCreationTime(ZoneId.of("UTC"))).isPresent();
        assertThat(incoming.getCreationTime(ZoneId.of("UTC")))
                .isEqualTo(incoming.getTimestamp(ZoneId.of("UTC")));
    }
}
