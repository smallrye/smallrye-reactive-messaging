package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.inject.Instance;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.ConnectionFactory;

import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.reactive.messaging.rabbitmq.og.internals.IncomingRabbitMQChannel;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

/**
 * Tests for consumer backpressure with max-outstanding-messages configuration.
 *
 * Configuration Highlights:
 * - max-outstanding-messages: Controls RabbitMQ QoS prefetch count
 * - Lower values increase backpressure, higher values allow more buffering
 *
 * Differences from Original Connector:
 * - Original uses CDI with @Channel injection and AssertSubscriber
 * - This test uses direct channel instantiation with manual subscription control
 * - Same underlying QoS mechanism (channel.basicQos)
 */
public class ConsumerBackPressureTest extends RabbitMQBrokerTestBase {

    @Test
    public void testBackpressureWithSmallPrefetch() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);

            // Configure with small prefetch to test backpressure
            // Use auto-ack to simplify backpressure testing (focus on QoS prefetch, not manual acks)
            TestIncomingConfig config = createBackpressureConfig(exchangeName, "backpressure-queue", true, 5);
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            // Use AssertSubscriber to control demand
            // With auto-ack, we can focus on testing demand/backpressure without ack complexity
            AssertSubscriber<Object> subscriber = AssertSubscriber.create(0); // Start with no demand
            channel.getStream()
                    .map(msg -> msg.getPayload()) // Extract payload (ack is automatic)
                    .subscribe().withSubscriber(subscriber);

            Thread.sleep(500); // Wait for consumer setup

            // Publish 100 messages
            for (int i = 0; i < 100; i++) {
                final int messageNum = i;
                usage.produce(exchangeName, null, "test.key", 1, () -> "Message-" + messageNum);
            }

            // Request 10 messages at a time
            subscriber.request(10);
            Thread.sleep(500);
            assertThat(subscriber.getItems()).hasSize(10);

            // Request 20 more
            subscriber.request(20);
            Thread.sleep(500);
            assertThat(subscriber.getItems()).hasSize(30);

            // Request the rest
            subscriber.request(70);
            Thread.sleep(1000);
            assertThat(subscriber.getItems()).hasSize(100);

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testBackpressureWithLargeBatch() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);

            // Use larger prefetch for batch processing (auto-ack for simplicity)
            TestIncomingConfig config = createBackpressureConfig(exchangeName, "batch-queue", true, 256);
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            AtomicInteger receivedCount = new AtomicInteger(0);
            AssertSubscriber<Object> subscriber = AssertSubscriber.create(0);

            channel.getStream()
                    .map(msg -> {
                        receivedCount.incrementAndGet();
                        return msg.getPayload();
                    })
                    .subscribe().withSubscriber(subscriber);

            Thread.sleep(500);

            // Publish 1000 messages
            for (int i = 0; i < 1000; i++) {
                final int messageNum = i;
                usage.produce(exchangeName, null, "test.key", 1, () -> "Batch-" + messageNum);
            }

            // Request in batches of 100
            for (int batch = 0; batch < 10; batch++) {
                subscriber.request(100);
                Thread.sleep(200);
            }

            assertThat(subscriber.getItems()).hasSize(1000);
            assertThat(receivedCount.get()).isEqualTo(1000);

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testBackpressureWithSlowConsumer() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);

            // Small prefetch to test slow consumer backpressure (manual ack for this test)
            TestIncomingConfig config = createBackpressureConfig(exchangeName, "slow-consumer-queue", false, 3);
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            AtomicInteger processedCount = new AtomicInteger(0);

            channel.getStream().subscribe().with(msg -> {
                // Simulate slow processing
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                msg.ack();
                processedCount.incrementAndGet();
            });

            Thread.sleep(500);

            // Publish 50 messages
            for (int i = 0; i < 50; i++) {
                final int messageNum = i;
                usage.produce(exchangeName, null, "test.key", 1, () -> "Slow-" + messageNum);
            }

            // With 50ms processing time and 50 messages, this should take at least 2.5 seconds
            // But due to prefetch=3, some messages can be buffered
            Thread.sleep(4000);

            assertThat(processedCount.get()).isEqualTo(50);

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testNoBackpressureWithUnlimitedPrefetch() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);

            // No max-outstanding-messages means unlimited prefetch (0 in RabbitMQ)
            TestIncomingConfig config = createUnlimitedPrefetchConfig(exchangeName, "unlimited-queue");
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            AtomicInteger receivedCount = new AtomicInteger(0);

            channel.getStream().subscribe().with(msg -> {
                receivedCount.incrementAndGet();
                msg.ack();
            });

            Thread.sleep(500);

            // Publish 500 messages - all should be fetched immediately
            for (int i = 0; i < 500; i++) {
                final int messageNum = i;
                usage.produce(exchangeName, null, "test.key", 1, () -> "Unlimited-" + messageNum);
            }

            // With no backpressure, all messages should arrive quickly
            Thread.sleep(2000);

            assertThat(receivedCount.get()).isEqualTo(500);

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    // Helper methods

    private TestIncomingConfig createBackpressureConfig(String exchangeName, String queueName, boolean autoAck,
            int maxOutstanding) {
        return new TestIncomingConfig(exchangeName, queueName, autoAck, maxOutstanding);
    }

    private TestIncomingConfig createUnlimitedPrefetchConfig(String exchangeName, String queueName) {
        return new UnlimitedPrefetchConfig(exchangeName, queueName);
    }

    // Test configuration for backpressure testing
    static class TestIncomingConfig extends RabbitMQConnectorIncomingConfiguration {
        private final String exchangeName;
        private final String queueName;
        private final boolean autoAck;
        private final int maxOutstanding;

        public TestIncomingConfig(String exchangeName, String queueName, boolean autoAck, int maxOutstanding) {
            super(createConfig());
            this.exchangeName = exchangeName;
            this.queueName = queueName;
            this.autoAck = autoAck;
            this.maxOutstanding = maxOutstanding;
        }

        private static MapBasedConfig createConfig() {
            Map<String, Object> configMap = new HashMap<>();
            configMap.put("channel-name", "test-channel");
            configMap.put("connector", "smallrye-rabbitmq-og");
            return new MapBasedConfig(configMap);
        }

        @Override
        public String getChannel() {
            return "test-channel";
        }

        @Override
        public java.util.Optional<String> getExchangeName() {
            return java.util.Optional.of(exchangeName);
        }

        @Override
        public Boolean getExchangeDeclare() {
            return true;
        }

        @Override
        public String getExchangeType() {
            return "topic";
        }

        @Override
        public Boolean getExchangeDurable() {
            return false;
        }

        @Override
        public Boolean getExchangeAutoDelete() {
            return true;
        }

        @Override
        public String getExchangeArguments() {
            return "rabbitmq-exchange-arguments";
        }

        @Override
        public java.util.Optional<String> getQueueName() {
            return java.util.Optional.of(queueName);
        }

        @Override
        public Boolean getQueueDeclare() {
            return true;
        }

        @Override
        public Boolean getQueueDurable() {
            return false;
        }

        @Override
        public Boolean getQueueExclusive() {
            return false;
        }

        @Override
        public Boolean getQueueAutoDelete() {
            return true;
        }

        @Override
        public String getQueueArguments() {
            return "rabbitmq-queue-arguments";
        }

        @Override
        public String getRoutingKeys() {
            return "#";
        }

        @Override
        public Boolean getAutoAcknowledgement() {
            return autoAck;
        }

        @Override
        public java.util.Optional<Integer> getMaxOutstandingMessages() {
            return java.util.Optional.of(maxOutstanding);
        }

        @Override
        public String getFailureStrategy() {
            return "reject";
        }

        @Override
        public Boolean getBroadcast() {
            return false;
        }

        @Override
        public Boolean getTracingEnabled() {
            return false;
        }
    }

    // Configuration for unlimited prefetch (no max-outstanding-messages)
    static class UnlimitedPrefetchConfig extends TestIncomingConfig {
        public UnlimitedPrefetchConfig(String exchangeName, String queueName) {
            super(exchangeName, queueName, false, 0);
        }

        @Override
        public java.util.Optional<Integer> getMaxOutstandingMessages() {
            return java.util.Optional.empty(); // No limit
        }
    }
}
