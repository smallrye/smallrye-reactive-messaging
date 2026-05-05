package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;

/**
 * Tests for RabbitMQMessageConverter
 */
public class RabbitMQMessageConverterTest {

    @Test
    public void testConvertStringPayload() {
        Message<String> message = Message.of("Hello RabbitMQ");

        RabbitMQMessageConverter.OutgoingRabbitMQMessage converted = RabbitMQMessageConverter.convert(
                message, "default.key", Optional.empty());

        assertThat(converted.getRoutingKey()).isEqualTo("default.key");
        assertThat(converted.getExchange()).isEmpty();
        assertThat(new String(converted.getBody(), StandardCharsets.UTF_8)).isEqualTo("Hello RabbitMQ");
        assertThat(converted.getProperties().getContentType()).isEqualTo("text/plain");
        assertThat(converted.getProperties().getDeliveryMode()).isEqualTo(2);
    }

    @Test
    public void testConvertByteArrayPayload() {
        byte[] data = "Binary data".getBytes(StandardCharsets.UTF_8);
        Message<byte[]> message = Message.of(data);

        RabbitMQMessageConverter.OutgoingRabbitMQMessage converted = RabbitMQMessageConverter.convert(
                message, "default.key", Optional.empty());

        assertThat(converted.getRoutingKey()).isEqualTo("default.key");
        assertThat(converted.getBody()).isEqualTo(data);
        assertThat(converted.getProperties().getContentType()).isEqualTo("application/octet-stream");
    }

    @Test
    public void testConvertWithMetadata() {
        OutgoingRabbitMQMetadata metadata = OutgoingRabbitMQMetadata.builder()
                .withRoutingKey("custom.routing.key")
                .withExchange("custom-exchange")
                .withContentType("application/json")
                .withPriority(5)
                .withTtl(60000)
                .withPersistent(true)
                .build();

        Message<String> message = Message.of("{\"test\":\"data\"}")
                .addMetadata(metadata);

        RabbitMQMessageConverter.OutgoingRabbitMQMessage converted = RabbitMQMessageConverter.convert(
                message, "default.key", Optional.empty());

        assertThat(converted.getRoutingKey()).isEqualTo("custom.routing.key");
        assertThat(converted.getExchange()).hasValue("custom-exchange");
        assertThat(converted.getProperties().getContentType()).isEqualTo("application/json");
        assertThat(converted.getProperties().getPriority()).isEqualTo(5);
        assertThat(converted.getProperties().getExpiration()).isEqualTo("60000");
        assertThat(converted.getProperties().getDeliveryMode()).isEqualTo(2);
    }

    @Test
    public void testConvertWithDefaultTtl() {
        Message<String> message = Message.of("Test message");

        RabbitMQMessageConverter.OutgoingRabbitMQMessage converted = RabbitMQMessageConverter.convert(
                message, "default.key", Optional.of(30000L));

        assertThat(converted.getProperties().getExpiration()).isEqualTo("30000");
    }

    @Test
    public void testConvertIntegerPayload() {
        Message<Integer> message = Message.of(42);

        RabbitMQMessageConverter.OutgoingRabbitMQMessage converted = RabbitMQMessageConverter.convert(
                message, "default.key", Optional.empty());

        assertThat(new String(converted.getBody(), StandardCharsets.UTF_8)).isEqualTo("42");
        assertThat(converted.getProperties().getContentType()).isEqualTo("text/plain");
    }

    @Test
    public void testConvertNullPayload() {
        Message<Object> message = Message.of(null);

        RabbitMQMessageConverter.OutgoingRabbitMQMessage converted = RabbitMQMessageConverter.convert(
                message, "default.key", Optional.empty());

        assertThat(converted.getBody()).isEmpty();
        assertThat(converted.getProperties().getContentType()).isEqualTo("application/octet-stream");
    }

    @Test
    public void testConvertFromIncomingMessage() {
        com.rabbitmq.client.Envelope envelope = new com.rabbitmq.client.Envelope(
                1L, false, "test-exchange", "test.key");

        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType("text/plain")
                .deliveryMode(2)
                .build();

        byte[] body = "Forwarded message".getBytes(StandardCharsets.UTF_8);

        IncomingRabbitMQMessage<byte[]> incomingMessage = IncomingRabbitMQMessage.create(
                envelope,
                props,
                body,
                IncomingRabbitMQMessage.BYTE_ARRAY_CONVERTER);

        RabbitMQMessageConverter.OutgoingRabbitMQMessage converted = RabbitMQMessageConverter.convert(
                incomingMessage, "default.key", Optional.empty());

        assertThat(converted.getRoutingKey()).isEqualTo("test.key");
        assertThat(converted.getExchange()).hasValue("test-exchange");
        assertThat(converted.getBody()).isEqualTo(body);
        assertThat(converted.getProperties().getContentType()).isEqualTo("text/plain");
    }

    @Test
    void convertWithUUIDPayload() {
        UUID uuid = UUID.randomUUID();
        Message<UUID> message = Message.of(uuid);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                message, "key", Optional.empty());

        assertThat(new String(result.getBody(), StandardCharsets.UTF_8)).isEqualTo(uuid.toString());
        assertThat(result.getProperties().getContentType()).isEqualTo("text/plain");
    }

    @Test
    void convertWithBooleanPayload() {
        Message<Boolean> message = Message.of(true);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                message, "key", Optional.empty());

        assertThat(new String(result.getBody(), StandardCharsets.UTF_8)).isEqualTo("true");
        assertThat(result.getProperties().getContentType()).isEqualTo("text/plain");
    }

    @Test
    void convertWithLongPayload() {
        Message<Long> message = Message.of(123456789L);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                message, "key", Optional.empty());

        assertThat(new String(result.getBody(), StandardCharsets.UTF_8)).isEqualTo("123456789");
        assertThat(result.getProperties().getContentType()).isEqualTo("text/plain");
    }

    @Test
    void convertWithDoublePayload() {
        Message<Double> message = Message.of(3.14);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                message, "key", Optional.empty());

        assertThat(new String(result.getBody(), StandardCharsets.UTF_8)).isEqualTo("3.14");
        assertThat(result.getProperties().getContentType()).isEqualTo("text/plain");
    }

    @Test
    void convertWithCharacterPayload() {
        Message<Character> message = Message.of('A');
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                message, "key", Optional.empty());

        assertThat(new String(result.getBody(), StandardCharsets.UTF_8)).isEqualTo("A");
        assertThat(result.getProperties().getContentType()).isEqualTo("text/plain");
    }

    @Test
    void convertWithPojoPayload() {
        Map<String, String> pojo = Map.of("foo", "bar");
        Message<Map<String, String>> message = Message.of(pojo);
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                message, "key", Optional.empty());

        assertThat(new String(result.getBody(), StandardCharsets.UTF_8)).contains("foo").contains("bar");
        assertThat(result.getProperties().getContentType()).isEqualTo("application/json");
    }

    @Test
    void convertWithoutMetadataAndNoDefaultTtl() {
        Message<String> message = Message.of("hello");
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                message, "default-key", Optional.empty());

        assertThat(result.getProperties().getExpiration()).isNull();
    }

    @Test
    void convertUsesRoutingKeyFromOutgoingMetadata() {
        OutgoingRabbitMQMetadata metadata = OutgoingRabbitMQMetadata.builder()
                .withRoutingKey("meta-routing-key")
                .build();
        Message<String> message = Message.of("data", Metadata.of(metadata));
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                message, "default-key", Optional.empty());

        assertThat(result.getRoutingKey()).isEqualTo("meta-routing-key");
    }

    @Test
    void convertFallsBackToDefaultRoutingKey() {
        Message<String> message = Message.of("data");
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                message, "fallback-key", Optional.empty());

        assertThat(result.getRoutingKey()).isEqualTo("fallback-key");
    }

    @Test
    void convertWithIncomingRabbitMQMessagePreservesProperties() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("x-custom", "val");
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType("text/plain")
                .contentEncoding("UTF-8")
                .headers(headers)
                .correlationId("corr-1")
                .expiration("3000")
                .build();
        com.rabbitmq.client.Envelope envelope = new com.rabbitmq.client.Envelope(
                1L, false, "exchange", "incoming-key");

        IncomingRabbitMQMessage<byte[]> incoming = IncomingRabbitMQMessage.create(
                envelope, props, "incoming-body".getBytes(StandardCharsets.UTF_8),
                IncomingRabbitMQMessage.BYTE_ARRAY_CONVERTER);

        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                incoming, "default-key", Optional.of(9999L));

        assertThat(result.getRoutingKey()).isEqualTo("incoming-key");
        assertThat(result.getProperties().getContentType()).isEqualTo("text/plain");
        assertThat(result.getProperties().getExpiration()).isEqualTo("3000");
        assertThat(result.getProperties().getCorrelationId()).isEqualTo("corr-1");
    }

    @Test
    void convertPreservesAllOutgoingMetadataProperties() {
        OutgoingRabbitMQMetadata metadata = OutgoingRabbitMQMetadata.builder()
                .withContentType("application/xml")
                .withContentEncoding("gzip")
                .withDeliveryMode(2)
                .withPriority(5)
                .withCorrelationId("c-id")
                .withReplyTo("reply-queue")
                .withExpiration("10000")
                .withMessageId("msg-id")
                .withType("my-type")
                .withUserId("user")
                .withAppId("app")
                .build();

        Message<String> message = Message.of("data", Metadata.of(metadata));
        RabbitMQMessageConverter.OutgoingRabbitMQMessage result = RabbitMQMessageConverter.convert(
                message, "key", Optional.empty());

        AMQP.BasicProperties props = result.getProperties();
        assertThat(props.getContentType()).isEqualTo("application/xml");
        assertThat(props.getContentEncoding()).isEqualTo("gzip");
        assertThat(props.getDeliveryMode()).isEqualTo(2);
        assertThat(props.getPriority()).isEqualTo(5);
        assertThat(props.getCorrelationId()).isEqualTo("c-id");
        assertThat(props.getReplyTo()).isEqualTo("reply-queue");
        assertThat(props.getExpiration()).isEqualTo("10000");
        assertThat(props.getMessageId()).isEqualTo("msg-id");
        assertThat(props.getType()).isEqualTo("my-type");
        assertThat(props.getUserId()).isEqualTo("user");
        assertThat(props.getAppId()).isEqualTo("app");
    }
}
