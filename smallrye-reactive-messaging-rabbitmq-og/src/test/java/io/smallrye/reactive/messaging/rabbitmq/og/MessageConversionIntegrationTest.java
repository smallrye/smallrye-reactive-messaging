package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

/**
 * CDI integration tests for message conversion with various payload types.
 * Tests verify that payloads are properly serialized with correct content-types
 * and can be deserialized correctly.
 */
public class MessageConversionIntegrationTest extends WeldTestBase {

    @Test
    void testStringPayloadConversion() {
        String exchangeName = "test-string-" + UUID.randomUUID().getMostSignificantBits();
        addBeans(StringConsumer.class);
        runApplication(commonConfig()
                .with("mp.messaging.incoming.string-in.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.string-in.exchange.name", exchangeName)
                .with("mp.messaging.incoming.string-in.exchange.type", "direct")
                .with("mp.messaging.incoming.string-in.queue.name", "string-queue")
                .with("mp.messaging.incoming.string-in.queue.declare", true)
                .with("mp.messaging.incoming.string-in.routing-keys", "string")
                .with("mp.messaging.incoming.string-in.host", host)
                .with("mp.messaging.incoming.string-in.port", port)
                .with("mp.messaging.incoming.string-in.tracing.enabled", false)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("rabbitmq-reconnect-attempts", 0));

        StringConsumer bean = get(StringConsumer.class);

        // Produce String messages externally
        java.util.concurrent.atomic.AtomicInteger counter1 = new java.util.concurrent.atomic.AtomicInteger();
        usage.produce(exchangeName, "string-queue", "string", 3, () -> "Message " + counter1.getAndIncrement());

        await().untilAsserted(() -> {
            assertThat(bean.getReceived()).hasSizeGreaterThanOrEqualTo(3);
        });

        List<ReceivedMessage> received = bean.getReceived();
        assertThat(received).extracting(ReceivedMessage::getPayload)
                .contains("Message 0", "Message 1", "Message 2");

        // Verify content-type is text/plain for String payloads
        assertThat(received).allMatch(msg -> "text/plain".equals(msg.getContentType()));
    }

    @Test
    void testIntegerPayloadConversion() {
        String exchangeName = "test-integer-" + UUID.randomUUID().getMostSignificantBits();
        addBeans(IntegerConsumer.class);
        runApplication(commonConfig()
                .with("mp.messaging.incoming.int-in.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.int-in.exchange.name", exchangeName)
                .with("mp.messaging.incoming.int-in.exchange.type", "direct")
                .with("mp.messaging.incoming.int-in.queue.name", "int-queue")
                .with("mp.messaging.incoming.int-in.queue.declare", true)
                .with("mp.messaging.incoming.int-in.routing-keys", "int")
                .with("mp.messaging.incoming.int-in.host", host)
                .with("mp.messaging.incoming.int-in.port", port)
                .with("mp.messaging.incoming.int-in.tracing.enabled", false)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("rabbitmq-reconnect-attempts", 0));

        IntegerConsumer bean = get(IntegerConsumer.class);

        // Produce Integer messages externally
        java.util.concurrent.atomic.AtomicInteger counter2 = new java.util.concurrent.atomic.AtomicInteger();
        usage.produce(exchangeName, "int-queue", "int", 5, () -> (counter2.getAndIncrement() + 1) * 10);

        await().untilAsserted(() -> {
            assertThat(bean.getReceived()).hasSizeGreaterThanOrEqualTo(5);
        });

        List<ReceivedMessage> received = bean.getReceived();
        assertThat(received).extracting(ReceivedMessage::getPayload)
                .contains("10", "20", "30", "40", "50");

        // Verify content-type is text/plain for Integer payloads
        assertThat(received).allMatch(msg -> "text/plain".equals(msg.getContentType()));
    }

    @Test
    void testByteArrayPayloadConversion() {
        String exchangeName = "test-bytes-" + UUID.randomUUID().getMostSignificantBits();
        addBeans(ByteArrayConsumer.class);
        runApplication(commonConfig()
                .with("mp.messaging.incoming.bytes-in.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.bytes-in.exchange.name", exchangeName)
                .with("mp.messaging.incoming.bytes-in.exchange.type", "direct")
                .with("mp.messaging.incoming.bytes-in.queue.name", "bytes-queue")
                .with("mp.messaging.incoming.bytes-in.queue.declare", true)
                .with("mp.messaging.incoming.bytes-in.routing-keys", "bytes")
                .with("mp.messaging.incoming.bytes-in.host", host)
                .with("mp.messaging.incoming.bytes-in.port", port)
                .with("mp.messaging.incoming.bytes-in.tracing.enabled", false)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("rabbitmq-reconnect-attempts", 0));

        ByteArrayConsumer bean = get(ByteArrayConsumer.class);

        // Produce messages with application/octet-stream content-type
        java.util.concurrent.atomic.AtomicInteger counter3 = new java.util.concurrent.atomic.AtomicInteger();
        usage.produce(exchangeName, "bytes-queue", "bytes", 3,
                () -> "Binary " + counter3.getAndIncrement(),
                "application/octet-stream");

        await().untilAsserted(() -> {
            assertThat(bean.getReceived()).hasSizeGreaterThanOrEqualTo(3);
        });

        List<ReceivedMessage> received = bean.getReceived();
        assertThat(received).extracting(ReceivedMessage::getPayload)
                .contains("Binary 0", "Binary 1", "Binary 2");

        // Verify content-type is application/octet-stream for byte[] payloads
        assertThat(received).allMatch(msg -> "application/octet-stream".equals(msg.getContentType()));
    }

    @Test
    void testJsonPayloadWithMetadata() {
        String exchangeName = "test-json-" + UUID.randomUUID().getMostSignificantBits();
        addBeans(JsonConsumer.class);
        runApplication(commonConfig()
                .with("mp.messaging.incoming.json-in.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.json-in.exchange.name", exchangeName)
                .with("mp.messaging.incoming.json-in.exchange.type", "direct")
                .with("mp.messaging.incoming.json-in.queue.name", "json-queue")
                .with("mp.messaging.incoming.json-in.queue.declare", true)
                .with("mp.messaging.incoming.json-in.routing-keys", "json")
                .with("mp.messaging.incoming.json-in.host", host)
                .with("mp.messaging.incoming.json-in.port", port)
                .with("mp.messaging.incoming.json-in.tracing.enabled", false)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("rabbitmq-reconnect-attempts", 0));

        JsonConsumer bean = get(JsonConsumer.class);

        // Produce JSON messages externally with explicit content-type
        java.util.concurrent.atomic.AtomicInteger counter4 = new java.util.concurrent.atomic.AtomicInteger();
        usage.produce(exchangeName, "json-queue", "json", 2,
                () -> {
                    int i = counter4.getAndIncrement();
                    return i == 0 ? "{\"id\":1,\"name\":\"Alice\"}" : "{\"id\":2,\"name\":\"Bob\"}";
                },
                "application/json");

        await().untilAsserted(() -> {
            assertThat(bean.getReceived()).hasSizeGreaterThanOrEqualTo(2);
        });

        List<ReceivedMessage> received = bean.getReceived();
        assertThat(received).extracting(ReceivedMessage::getPayload)
                .contains("{\"id\":1,\"name\":\"Alice\"}", "{\"id\":2,\"name\":\"Bob\"}");

        // Verify content-type is application/json when set via metadata
        assertThat(received).allMatch(msg -> "application/json".equals(msg.getContentType()));
    }

    // Helper class to capture received message details
    public static class ReceivedMessage {
        private final String payload;
        private final String contentType;

        public ReceivedMessage(String payload, String contentType) {
            this.payload = payload;
            this.contentType = contentType;
        }

        public String getPayload() {
            return payload;
        }

        public String getContentType() {
            return contentType;
        }
    }

    @ApplicationScoped
    public static class StringConsumer {
        private final List<ReceivedMessage> received = new CopyOnWriteArrayList<>();

        @Incoming("string-in")
        public CompletionStage<Void> consume(Message<byte[]> message) {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            String contentType = message.getMetadata(IncomingRabbitMQMetadata.class)
                    .map(IncomingRabbitMQMetadata::getContentType)
                    .orElse("unknown");
            received.add(new ReceivedMessage(payload, contentType));
            return message.ack();
        }

        public List<ReceivedMessage> getReceived() {
            return received;
        }
    }

    @ApplicationScoped
    public static class IntegerConsumer {
        private final List<ReceivedMessage> received = new CopyOnWriteArrayList<>();

        @Incoming("int-in")
        public CompletionStage<Void> consume(Message<byte[]> message) {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            String contentType = message.getMetadata(IncomingRabbitMQMetadata.class)
                    .map(IncomingRabbitMQMetadata::getContentType)
                    .orElse("unknown");
            received.add(new ReceivedMessage(payload, contentType));
            return message.ack();
        }

        public List<ReceivedMessage> getReceived() {
            return received;
        }
    }

    @ApplicationScoped
    public static class ByteArrayConsumer {
        private final List<ReceivedMessage> received = new CopyOnWriteArrayList<>();

        @Incoming("bytes-in")
        public CompletionStage<Void> consume(Message<byte[]> message) {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            String contentType = message.getMetadata(IncomingRabbitMQMetadata.class)
                    .map(IncomingRabbitMQMetadata::getContentType)
                    .orElse("unknown");
            received.add(new ReceivedMessage(payload, contentType));
            return message.ack();
        }

        public List<ReceivedMessage> getReceived() {
            return received;
        }
    }

    @ApplicationScoped
    public static class JsonConsumer {
        private final List<ReceivedMessage> received = new CopyOnWriteArrayList<>();

        @Incoming("json-in")
        public CompletionStage<Void> consume(Message<byte[]> message) {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            String contentType = message.getMetadata(IncomingRabbitMQMetadata.class)
                    .map(IncomingRabbitMQMetadata::getContentType)
                    .orElse("unknown");
            received.add(new ReceivedMessage(payload, contentType));
            return message.ack();
        }

        public List<ReceivedMessage> getReceived() {
            return received;
        }
    }
}
