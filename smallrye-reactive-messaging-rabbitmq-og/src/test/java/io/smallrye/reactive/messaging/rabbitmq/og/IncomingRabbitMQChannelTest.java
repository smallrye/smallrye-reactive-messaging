package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.ConnectionFactory;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.rabbitmq.og.internals.IncomingRabbitMQChannel;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

/**
 * Tests for IncomingRabbitMQChannel configuration
 */
public class IncomingRabbitMQChannelTest extends RabbitMQBrokerTestBase {

    @Test
    public void testBasicMessageConsumption() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            RabbitMQConnectorIncomingConfiguration config = createTestConfig(exchangeName, "test-queue", false, 256);
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            List<Message<?>> received = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(5);

            Multi<? extends Message<?>> stream = channel.getStream();
            stream.subscribe().with(msg -> {
                received.add(msg);
                msg.ack();
                latch.countDown();
            });

            // Wait for consumer to be ready
            Thread.sleep(500);

            // Publish messages
            for (int i = 0; i < 5; i++) {
                final int messageNum = i;
                usage.produce(exchangeName, null, "test.key", 1, () -> "Message " + messageNum);
            }

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(received).hasSize(5);

            for (int i = 0; i < 5; i++) {
                byte[] payload = (byte[]) received.get(i).getPayload();
                assertThat(new String(payload, StandardCharsets.UTF_8)).isEqualTo("Message " + i);
            }

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testAutoAcknowledgement() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            RabbitMQConnectorIncomingConfiguration config = createAutoAckConfig(exchangeName, "auto-ack-queue");
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            List<String> received = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(3);

            channel.getStream().subscribe().with(msg -> {
                byte[] payload = (byte[]) msg.getPayload();
                received.add(new String(payload, StandardCharsets.UTF_8));
                latch.countDown();
                // Don't call ack - should be auto-acked
            });

            Thread.sleep(500);

            usage.produce(exchangeName, null, "test.key", 1, () -> "Auto-Ack-1");
            usage.produce(exchangeName, null, "test.key", 1, () -> "Auto-Ack-2");
            usage.produce(exchangeName, null, "test.key", 1, () -> "Auto-Ack-3");

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(received).containsExactly("Auto-Ack-1", "Auto-Ack-2", "Auto-Ack-3");

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testManualAcknowledgement() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            RabbitMQConnectorIncomingConfiguration config = createTestConfig(exchangeName, "manual-ack-queue", false, 256);
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            AtomicInteger ackCount = new AtomicInteger(0);
            CountDownLatch receiveLatch = new CountDownLatch(3);
            CountDownLatch ackLatch = new CountDownLatch(3);

            channel.getStream().subscribe().with(msg -> {
                receiveLatch.countDown();
                // Manually ack
                msg.ack().whenComplete((v, t) -> {
                    ackCount.incrementAndGet();
                    ackLatch.countDown();
                });
            });

            Thread.sleep(500);

            usage.produce(exchangeName, null, "test.key", 1, () -> "Manual-1");
            usage.produce(exchangeName, null, "test.key", 1, () -> "Manual-2");
            usage.produce(exchangeName, null, "test.key", 1, () -> "Manual-3");

            assertThat(receiveLatch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(ackLatch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(ackCount.get()).isEqualTo(3);

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testMessageMetadata() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            RabbitMQConnectorIncomingConfiguration config = createTestConfig(exchangeName, "metadata-queue", false, 256);
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            CountDownLatch latch = new CountDownLatch(1);
            List<IncomingRabbitMQMetadata> metadataList = new CopyOnWriteArrayList<>();

            channel.getStream().subscribe().with(msg -> {
                IncomingRabbitMQMetadata metadata = msg.getMetadata(IncomingRabbitMQMetadata.class)
                        .orElse(null);
                if (metadata != null) {
                    metadataList.add(metadata);
                }
                msg.ack();
                latch.countDown();
            });

            Thread.sleep(500);

            usage.produce(exchangeName, null, "test.routing.key", 1, () -> "Metadata Test");

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(metadataList).hasSize(1);

            IncomingRabbitMQMetadata metadata = metadataList.get(0);
            assertThat(metadata.getRoutingKey()).isEqualTo("test.routing.key");
            assertThat(metadata.getExchange()).isEqualTo(exchangeName);
            assertThat(metadata.getContentType()).isNotNull();

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testHealthyChannel() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            RabbitMQConnectorIncomingConfiguration config = createTestConfig(exchangeName, "health-queue", false, 256);
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            // Trigger stream initialization
            channel.getStream().subscribe().with(msg -> msg.ack());

            Thread.sleep(500);

            assertThat(channel.isHealthy()).isTrue();

            holder.close();

            Thread.sleep(100);
            assertThat(channel.isHealthy()).isFalse();
        } finally {
            vertx.closeAndAwait();
        }
    }

    // Helper methods to create test configurations

    static TestIncomingConfig createTestConfig(String exchangeName, String queueName, boolean autoAck,
            int maxOutstanding) {
        return new TestIncomingConfig(exchangeName, queueName, autoAck, maxOutstanding);
    }

    private TestIncomingConfig createAutoAckConfig(String exchangeName, String queueName) {
        return new TestIncomingConfig(exchangeName, queueName, true, 256);
    }

    // Simple test configuration implementation
    static class TestIncomingConfig extends RabbitMQConnectorIncomingConfiguration {

        private final String exchangeName;
        private final String queueName;
        private final boolean autoAck;
        private final int maxOutstanding;

        public TestIncomingConfig(String exchangeName, String queueName, boolean autoAck, int maxOutstanding) {
            super(createConfig(exchangeName, queueName, autoAck, maxOutstanding));
            this.exchangeName = exchangeName;
            this.queueName = queueName;
            this.autoAck = autoAck;
            this.maxOutstanding = maxOutstanding;
        }

        private static MapBasedConfig createConfig(String exchangeName, String queueName, boolean autoAck, int maxOutstanding) {
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
}
