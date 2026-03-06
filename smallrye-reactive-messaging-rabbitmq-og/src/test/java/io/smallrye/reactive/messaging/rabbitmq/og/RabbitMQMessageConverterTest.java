package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.eclipse.microprofile.reactive.messaging.Message;
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
        assertThat(converted.getProperties().getDeliveryMode()).isEqualTo(2); // Persistent
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
        // Create an incoming message
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

        // Convert it for forwarding
        RabbitMQMessageConverter.OutgoingRabbitMQMessage converted = RabbitMQMessageConverter.convert(
                incomingMessage, "default.key", Optional.empty());

        assertThat(converted.getRoutingKey()).isEqualTo("test.key"); // Uses original routing key
        assertThat(converted.getExchange()).hasValue("test-exchange"); // Uses original exchange
        assertThat(converted.getBody()).isEqualTo(body);
        assertThat(converted.getProperties().getContentType()).isEqualTo("text/plain");
    }
}
