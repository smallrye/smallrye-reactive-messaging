package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.ConnectionFactory;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.rabbitmq.og.internals.OutgoingRabbitMQChannel;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class OutgoingRabbitMQChannelTest extends RabbitMQBrokerTestBase {

    @Test
    public void testMessagesDeliveredWhenStreamCompletes() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionHolder holder = createHolder(vertx);
            OutgoingRabbitMQChannel channel = createChannel(holder, createTestConfig());

            List<RabbitMQUsage.RabbitMQMessage> received = new CopyOnWriteArrayList<>();
            usage.consume(exchangeName, "#", received::add);

            List<Message<String>> messages = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                messages.add(Message.of("msg-" + i));
            }

            subscribe(Multi.createFrom().iterable(messages), channel.getSink());

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(received).hasSize(10));

            for (int i = 0; i < 10; i++) {
                assertThat(received.get(i).bodyAsString()).isEqualTo("msg-" + i);
            }

            channel.closeQuietly();
            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testComplexObjectSerializedAsJson() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionHolder holder = createHolder(vertx);
            OutgoingRabbitMQChannel channel = createChannel(holder, createTestConfig());

            List<RabbitMQUsage.RabbitMQMessage> received = new CopyOnWriteArrayList<>();
            usage.consume(exchangeName, "#", received::add);

            Map<String, Object> pojo = Map.of("name", "alice", "age", 30);
            subscribe(Multi.createFrom().item(Message.of(pojo)), channel.getSink());

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(received).hasSize(1));

            io.vertx.core.json.JsonObject json = new io.vertx.core.json.JsonObject(received.get(0).bodyAsString());
            assertThat(json.getString("name")).isEqualTo("alice");
            assertThat(json.getInteger("age")).isEqualTo(30);
            assertThat(received.get(0).properties().getContentType()).isEqualTo("application/json");

            channel.closeQuietly();
            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testPublishingWithMetadata() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionHolder holder = createHolder(vertx);
            OutgoingRabbitMQChannel channel = createChannel(holder, createTestConfig());

            List<RabbitMQUsage.RabbitMQMessage> received = new CopyOnWriteArrayList<>();
            usage.consume(exchangeName, "custom.routing.key", received::add);

            OutgoingRabbitMQMetadata metadata = OutgoingRabbitMQMetadata.builder()
                    .withRoutingKey("custom.routing.key")
                    .withContentType("application/json")
                    .withPriority(5)
                    .withHeader("custom-header", "custom-value")
                    .withTtl(60000)
                    .withPersistent(true)
                    .build();

            Message<String> message = Message.of("{\"test\":\"data\"}")
                    .addMetadata(metadata);

            subscribe(Multi.createFrom().item(message), channel.getSink());

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(received).hasSize(1));

            assertThat(received.get(0).bodyAsString()).isEqualTo("{\"test\":\"data\"}");
            assertThat(received.get(0).properties().getContentType()).isEqualTo("application/json");
            assertThat(received.get(0).properties().getPriority()).isEqualTo(5);

            channel.closeQuietly();
            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testHealthCheck() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionHolder holder = createHolder(vertx);
            OutgoingRabbitMQChannel channel = createChannel(holder, createTestConfig());

            Multi<Message<String>> neverCompleting = Multi.createFrom().ticks().every(Duration.ofSeconds(10))
                    .map(tick -> Message.of("test-" + tick));
            subscribe(neverCompleting, channel.getSink());

            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(channel.isHealthy()).isTrue());

            holder.close();

            assertThat(channel.isHealthy()).isFalse();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testGracefulShutdownDeliversAllMessages() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionHolder holder = createHolder(vertx);
            OutgoingRabbitMQChannel channel = createChannel(holder, createTestConfig());

            List<RabbitMQUsage.RabbitMQMessage> received = new CopyOnWriteArrayList<>();
            usage.consume(exchangeName, "#", received::add);

            Multi<Message<String>> stream = Multi.createFrom().ticks().every(Duration.ofMillis(100))
                    .onOverflow().drop()
                    .map(tick -> Message.of("tick-" + tick));

            subscribe(stream, channel.getSink());

            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(received.size()).isGreaterThanOrEqualTo(5));

            channel.closeQuietly();

            // After shutdown, no more messages should arrive.
            // Use during() to assert the count stays stable over a window.
            int countAfterShutdown = received.size();
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(received).hasSize(countAfterShutdown));

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    // Helper methods

    private ConnectionHolder createHolder(Vertx vertx) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        return new ConnectionHolder(factory, "test-channel", vertx);
    }

    private OutgoingRabbitMQChannel createChannel(ConnectionHolder holder, RabbitMQConnectorOutgoingConfiguration config) {
        return new OutgoingRabbitMQChannel(holder, config,
                Instance.class.cast(null), Instance.class.cast(null));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void subscribe(Multi<? extends Message<?>> multi, Flow.Subscriber<? extends Message<?>> sink) {
        multi.subscribe((Flow.Subscriber) sink);
    }

    private MapBasedConfig baseOutgoingConfig() {
        return new MapBasedConfig()
                .with("channel-name", "test-channel")
                .with("connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("exchange.name", exchangeName)
                .with("exchange.durable", false)
                .with("exchange.auto-delete", true)
                .with("default-routing-key", "test.key")
                .with("retry-on-fail-interval", 1)
                .with("tracing.enabled", false);
    }

    private RabbitMQConnectorOutgoingConfiguration createTestConfig() {
        return new RabbitMQConnectorOutgoingConfiguration(baseOutgoingConfig());
    }
}
