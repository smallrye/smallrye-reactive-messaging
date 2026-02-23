package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAckHandler;
import io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQFailureHandler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Context;

class RabbitMQMessageConverterTest {

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

    // --- convert: plain message (no RabbitMQMessage), various payload types ---

    @Test
    void convertWithStringPayload() {
        Message<String> message = Message.of("hello");
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "default-key", Optional.empty(), false);

        assertThat(result.getBody().toString()).isEqualTo("hello");
        assertThat(result.getRoutingKey()).isEqualTo("default-key");
        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.TEXT_PLAIN.toString());
    }

    @Test
    void convertWithNullPayload() {
        Message<Object> message = Message.of(null);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "key", Optional.empty(), false);

        assertThat(result.getBody().length()).isZero();
        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.APPLICATION_OCTET_STREAM.toString());
    }

    @Test
    void convertWithUUIDPayload() {
        UUID uuid = UUID.randomUUID();
        Message<UUID> message = Message.of(uuid);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "key", Optional.empty(), false);

        assertThat(result.getBody().toString()).isEqualTo(uuid.toString());
        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.TEXT_PLAIN.toString());
    }

    @Test
    void convertWithIntegerPayload() {
        Message<Integer> message = Message.of(42);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "key", Optional.empty(), false);

        assertThat(result.getBody().toString()).isEqualTo("42");
        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.TEXT_PLAIN.toString());
    }

    @Test
    void convertWithBooleanPayload() {
        Message<Boolean> message = Message.of(true);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "key", Optional.empty(), false);

        assertThat(result.getBody().toString()).isEqualTo("true");
        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.TEXT_PLAIN.toString());
    }

    @Test
    void convertWithVertxCoreBufferPayload() {
        io.vertx.core.buffer.Buffer coreBuffer = io.vertx.core.buffer.Buffer.buffer("core-buffer");
        Message<io.vertx.core.buffer.Buffer> message = Message.of(coreBuffer);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "key", Optional.empty(), false);

        assertThat(result.getBody().toString()).isEqualTo("core-buffer");
        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.APPLICATION_OCTET_STREAM.toString());
    }

    @Test
    void convertWithByteArrayPayload() {
        byte[] bytes = "bytes".getBytes();
        Message<byte[]> message = Message.of(bytes);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "key", Optional.empty(), false);

        assertThat(result.getBody().toString()).isEqualTo("bytes");
        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.APPLICATION_OCTET_STREAM.toString());
    }

    @Test
    void convertWithJsonObjectPayload() {
        JsonObject json = new JsonObject().put("key", "value");
        Message<JsonObject> message = Message.of(json);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "key", Optional.empty(), false);

        assertThat(result.getBody().toString()).isEqualTo(json.encode());
        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.APPLICATION_JSON.toString());
    }

    @Test
    void convertWithJsonArrayPayload() {
        JsonArray array = new JsonArray().add("a").add("b");
        Message<JsonArray> message = Message.of(array);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "key", Optional.empty(), false);

        assertThat(result.getBody().toString()).isEqualTo(array.encode());
        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.APPLICATION_JSON.toString());
    }

    @Test
    void convertWithPojoPayload() {
        // A plain object should be JSON-serialized
        Map<String, String> pojo = Map.of("foo", "bar");
        Message<Map<String, String>> message = Message.of(pojo);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "key", Optional.empty(), false);

        assertThat(result.getBody().toString()).contains("foo").contains("bar");
        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.APPLICATION_JSON.toString());
    }

    // --- convert: with OutgoingRabbitMQMetadata ---

    @Test
    void convertWithOutgoingMetadataAndTimestamp() {
        ZonedDateTime ts = ZonedDateTime.of(2024, 1, 15, 12, 0, 0, 0, ZoneOffset.UTC);
        OutgoingRabbitMQMetadata metadata = new OutgoingRabbitMQMetadata.Builder()
                .withTimestamp(ts)
                .withContentType("text/plain")
                .withCorrelationId("corr-123")
                .withRoutingKey("meta-key")
                .build();

        Message<String> message = Message.of("payload", Metadata.of(metadata));
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "default-key", Optional.empty(), false);

        assertThat(result.getRoutingKey()).isEqualTo("meta-key");
        assertThat(result.getProperties().getContentType()).isEqualTo("text/plain");
        assertThat(result.getProperties().getCorrelationId()).isEqualTo("corr-123");
        assertThat(result.getProperties().getTimestamp()).isNotNull();
    }

    @Test
    void convertWithOutgoingMetadataWithoutTimestamp() {
        OutgoingRabbitMQMetadata metadata = new OutgoingRabbitMQMetadata.Builder()
                .withContentType("application/xml")
                .build();

        Message<String> message = Message.of("payload", Metadata.of(metadata));
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "default-key", Optional.empty(), false);

        assertThat(result.getProperties().getTimestamp()).isNull();
        assertThat(result.getProperties().getContentType()).isEqualTo("application/xml");
    }

    @Test
    void convertWithoutOutgoingMetadataFallsBackToDefaults() {
        Message<String> message = Message.of("hello");
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "default-key", Optional.of(5000L), false);

        // No OutgoingRabbitMQMetadata → creates default with defaultTtl
        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.TEXT_PLAIN.toString());
        assertThat(result.getProperties().getExpiration()).isEqualTo("5000");
    }

    @Test
    void convertWithoutMetadataAndNoDefaultTtl() {
        Message<String> message = Message.of("hello");
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "default-key", Optional.empty(), false);

        assertThat(result.getProperties().getExpiration()).isNull();
    }

    @Test
    void convertMetadataContentTypeFallsBackToDefault() {
        // Metadata with null contentType → falls back to default for payload type
        OutgoingRabbitMQMetadata metadata = new OutgoingRabbitMQMetadata.Builder().build();
        Message<String> message = Message.of("hello", Metadata.of(metadata));

        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "default-key", Optional.empty(), false);

        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.TEXT_PLAIN.toString());
    }

    // --- convert: with IncomingRabbitMQMessage (RabbitMQMessage present) ---

    @Test
    void convertWithIncomingRabbitMQMessage() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("x-custom", "val");
        BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType("text/plain")
                .contentEncoding("UTF-8")
                .headers(headers)
                .correlationId("corr-1")
                .expiration("3000")
                .build();
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .body("incoming-body")
                .properties(props)
                .envelope(1L, false, "exchange", "incoming-key")
                .build();

        IncomingRabbitMQMessage<io.vertx.core.buffer.Buffer> incoming = new IncomingRabbitMQMessage<>(testMsg,
                null, null,
                doNothingNack,
                doNothingAck,
                null);

        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, incoming, "exchange", "default-key", Optional.of(9999L), false);

        // Routing key comes from the envelope
        assertThat(result.getRoutingKey()).isEqualTo("incoming-key");
        // Content type from source properties
        assertThat(result.getProperties().getContentType()).isEqualTo("text/plain");
        // Expiration from source properties (not overridden by default TTL since it's already set)
        assertThat(result.getProperties().getExpiration()).isEqualTo("3000");
        assertThat(result.getProperties().getCorrelationId()).isEqualTo("corr-1");
    }

    @Test
    void convertWithIncomingRabbitMQMessageNoExpirationUsesDefaultTtl() {
        Map<String, Object> headers = new HashMap<>();
        BasicProperties props = new AMQP.BasicProperties.Builder()
                .headers(headers)
                .build();
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .body("body")
                .properties(props)
                .envelope(1L, false, "exchange", "rk")
                .build();

        IncomingRabbitMQMessage<io.vertx.core.buffer.Buffer> incoming = new IncomingRabbitMQMessage<>(testMsg,
                null, null,
                doNothingNack,
                doNothingAck,
                null);

        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, incoming, "exchange", "default-key", Optional.of(7000L), false);

        // No expiration on source → should use default TTL
        assertThat(result.getProperties().getExpiration()).isEqualTo("7000");
    }

    @Test
    void convertWithIncomingRabbitMQMessageNoContentTypeFallsBackToDefault() {
        Map<String, Object> headers = new HashMap<>();
        BasicProperties props = new AMQP.BasicProperties.Builder()
                .headers(headers)
                .build();
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .body("body")
                .properties(props)
                .envelope(1L, false, "exchange", "rk")
                .build();

        IncomingRabbitMQMessage<io.vertx.core.buffer.Buffer> incoming = new IncomingRabbitMQMessage<>(testMsg,
                null, null,
                doNothingNack,
                doNothingAck,
                null);

        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, incoming, "exchange", "default-key", Optional.empty(), false);

        // No content type on source → falls back to default for payload type
        assertThat(result.getProperties().getContentType()).isNotNull();
    }

    // --- convert: with mutiny RabbitMQMessage payload ---

    @Test
    void convertWithMutinyRabbitMQMessagePayload() {
        Map<String, Object> headers = new HashMap<>();
        BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .headers(headers)
                .build();
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .body("mutiny-payload")
                .properties(props)
                .envelope(1L, false, "ex", "mutiny-rk")
                .build();

        io.vertx.mutiny.rabbitmq.RabbitMQMessage mutinyMsg = io.vertx.mutiny.rabbitmq.RabbitMQMessage
                .newInstance(testMsg);

        // Message wrapping a mutiny RabbitMQMessage as payload (not IncomingRabbitMQMessage)
        Message<io.vertx.mutiny.rabbitmq.RabbitMQMessage> message = Message.of(mutinyMsg);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "default-key", Optional.empty(), false);

        assertThat(result.getRoutingKey()).isEqualTo("mutiny-rk");
        assertThat(result.getProperties().getContentType()).isEqualTo("application/json");
    }

    // --- convert: with core RabbitMQMessage payload ---

    @Test
    void convertWithCoreRabbitMQMessagePayload() {
        Map<String, Object> headers = new HashMap<>();
        BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType("text/xml")
                .headers(headers)
                .build();
        io.vertx.rabbitmq.RabbitMQMessage testMsg = TestRabbitMQMessage.builder()
                .body("core-payload")
                .properties(props)
                .envelope(2L, false, "ex", "core-rk")
                .build();

        // Message wrapping a core (non-mutiny) RabbitMQMessage as payload
        Message<io.vertx.rabbitmq.RabbitMQMessage> message = Message.of(testMsg);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "default-key", Optional.empty(), false);

        assertThat(result.getRoutingKey()).isEqualTo("core-rk");
        assertThat(result.getProperties().getContentType()).isEqualTo("text/xml");
    }

    // --- getRoutingKey: from OutgoingRabbitMQMetadata ---

    @Test
    void convertUsesRoutingKeyFromOutgoingMetadata() {
        OutgoingRabbitMQMetadata metadata = new OutgoingRabbitMQMetadata.Builder()
                .withRoutingKey("meta-routing-key")
                .build();
        Message<String> message = Message.of("data", Metadata.of(metadata));
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "default-key", Optional.empty(), false);

        assertThat(result.getRoutingKey()).isEqualTo("meta-routing-key");
    }

    @Test
    void convertFallsBackToDefaultRoutingKey() {
        Message<String> message = Message.of("data");
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "fallback-key", Optional.empty(), false);

        assertThat(result.getRoutingKey()).isEqualTo("fallback-key");
    }

    // --- convert: Long and other primitive types ---

    @Test
    void convertWithLongPayload() {
        Message<Long> message = Message.of(123456789L);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "key", Optional.empty(), false);

        assertThat(result.getBody().toString()).isEqualTo("123456789");
        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.TEXT_PLAIN.toString());
    }

    @Test
    void convertWithDoublePayload() {
        Message<Double> message = Message.of(3.14);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "key", Optional.empty(), false);

        assertThat(result.getBody().toString()).isEqualTo("3.14");
        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.TEXT_PLAIN.toString());
    }

    @Test
    void convertWithCharacterPayload() {
        Message<Character> message = Message.of('A');
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "key", Optional.empty(), false);

        assertThat(result.getBody().toString()).isEqualTo("A");
        assertThat(result.getProperties().getContentType())
                .isEqualTo(HttpHeaderValues.TEXT_PLAIN.toString());
    }

    // --- Metadata properties preservation ---

    @Test
    void convertPreservesAllOutgoingMetadataProperties() {
        ZonedDateTime ts = ZonedDateTime.of(2024, 6, 1, 10, 30, 0, 0, ZoneOffset.UTC);
        OutgoingRabbitMQMetadata metadata = new OutgoingRabbitMQMetadata.Builder()
                .withContentType("application/xml")
                .withContentEncoding("gzip")
                .withDeliveryMode(2)
                .withPriority(5)
                .withCorrelationId("c-id")
                .withReplyTo("reply-queue")
                .withExpiration("10000")
                .withMessageId("msg-id")
                .withTimestamp(ts)
                .withType("my-type")
                .withUserId("user")
                .withAppId("app")
                .withClusterId("cluster")
                .build();

        Message<String> message = Message.of("data", Metadata.of(metadata));
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                null, message, "exchange", "key", Optional.empty(), false);

        BasicProperties props = result.getProperties();
        assertThat(props.getContentType()).isEqualTo("application/xml");
        assertThat(props.getContentEncoding()).isEqualTo("gzip");
        assertThat(props.getDeliveryMode()).isEqualTo(2);
        assertThat(props.getPriority()).isEqualTo(5);
        assertThat(props.getCorrelationId()).isEqualTo("c-id");
        assertThat(props.getReplyTo()).isEqualTo("reply-queue");
        assertThat(props.getExpiration()).isEqualTo("10000");
        assertThat(props.getMessageId()).isEqualTo("msg-id");
        assertThat(props.getTimestamp()).isNotNull();
        assertThat(props.getType()).isEqualTo("my-type");
        assertThat(props.getUserId()).isEqualTo("user");
        assertThat(props.getAppId()).isEqualTo("app");
        // clusterId is set in OutgoingRabbitMQMetadata but not exposed by BasicProperties interface
    }
}
