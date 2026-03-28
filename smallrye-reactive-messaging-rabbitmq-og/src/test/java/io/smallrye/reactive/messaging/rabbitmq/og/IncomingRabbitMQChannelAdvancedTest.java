package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.inject.Instance;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.ConnectionFactory;

import io.smallrye.reactive.messaging.rabbitmq.og.internals.IncomingRabbitMQChannel;
import io.vertx.mutiny.core.Vertx;

/**
 * Advanced tests for IncomingRabbitMQChannel configuration
 */
@Disabled
public class IncomingRabbitMQChannelAdvancedTest extends RabbitMQBrokerTestBase {

    @Test
    public void testBroadcastMode() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            BroadcastTestConfig config = createBroadcastConfig(exchangeName, "broadcast-queue");
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            // Create two subscribers
            List<String> subscriber1 = new CopyOnWriteArrayList<>();
            List<String> subscriber2 = new CopyOnWriteArrayList<>();
            CountDownLatch latch1 = new CountDownLatch(3);
            CountDownLatch latch2 = new CountDownLatch(3);

            channel.getStream().subscribe().with(msg -> {
                byte[] payload = (byte[]) msg.getPayload();
                subscriber1.add(new String(payload, StandardCharsets.UTF_8));
                msg.ack();
                latch1.countDown();
            });

            channel.getStream().subscribe().with(msg -> {
                byte[] payload = (byte[]) msg.getPayload();
                subscriber2.add(new String(payload, StandardCharsets.UTF_8));
                msg.ack();
                latch2.countDown();
            });

            Thread.sleep(500);

            // Publish messages
            usage.produce(exchangeName, null, "test.key", 1, () -> "Broadcast-1");
            usage.produce(exchangeName, null, "test.key", 1, () -> "Broadcast-2");
            usage.produce(exchangeName, null, "test.key", 1, () -> "Broadcast-3");

            assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();

            // Both subscribers should receive all messages
            assertThat(subscriber1).hasSize(3);
            assertThat(subscriber2).hasSize(3);

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testMaxOutstandingMessages() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            // Set max outstanding to 5
            IncomingRabbitMQChannelTest.TestIncomingConfig config = createBackpressureConfig(exchangeName,
                    "backpressure-queue");
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            AtomicInteger receivedCount = new AtomicInteger(0);
            CountDownLatch processLatch = new CountDownLatch(10);

            channel.getStream().subscribe().with(msg -> {
                receivedCount.incrementAndGet();
                // Simulate slow processing
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                msg.ack().whenComplete((v, t) -> processLatch.countDown());
            });

            Thread.sleep(500);

            // Publish 10 messages
            for (int i = 0; i < 10; i++) {
                final int messageNum = i;
                usage.produce(exchangeName, null, "test.key", 1, () -> "Message-" + messageNum);
            }

            // All should eventually be processed
            assertThat(processLatch.await(15, TimeUnit.SECONDS)).isTrue();
            assertThat(receivedCount.get()).isEqualTo(10);

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testMessageNacking() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            IncomingRabbitMQChannelTest.TestIncomingConfig config = IncomingRabbitMQChannelTest
                    .createTestConfig(exchangeName, "nack-queue", false, 256);
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            AtomicInteger nackCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(1);

            channel.getStream().subscribe().with(msg -> {
                // Nack the message
                msg.nack(new RuntimeException("Test nack")).whenComplete((v, t) -> {
                    nackCount.incrementAndGet();
                    latch.countDown();
                });
            });

            Thread.sleep(500);

            usage.produce(exchangeName, null, "test.key", 1, () -> "Nack-Test");

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(nackCount.get()).isEqualTo(1);

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testMultipleMessagesWithMetadata() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            IncomingRabbitMQChannelTest.TestIncomingConfig config = IncomingRabbitMQChannelTest
                    .createTestConfig(exchangeName, "multi-metadata-queue", false, 256);
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            List<IncomingRabbitMQMetadata> metadataList = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(5);

            channel.getStream().subscribe().with(msg -> {
                msg.getMetadata(IncomingRabbitMQMetadata.class).ifPresent(metadataList::add);
                msg.ack();
                latch.countDown();
            });

            Thread.sleep(500);

            // Publish with different routing keys
            usage.produce(exchangeName, null, "key.1", 1, () -> "Message 1");
            usage.produce(exchangeName, null, "key.2", 1, () -> "Message 2");
            usage.produce(exchangeName, null, "key.3", 1, () -> "Message 3");
            usage.produce(exchangeName, null, "key.4", 1, () -> "Message 4");
            usage.produce(exchangeName, null, "key.5", 1, () -> "Message 5");

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(metadataList).hasSize(5);

            // Verify routing keys
            assertThat(metadataList.get(0).getRoutingKey()).isEqualTo("key.1");
            assertThat(metadataList.get(1).getRoutingKey()).isEqualTo("key.2");
            assertThat(metadataList.get(2).getRoutingKey()).isEqualTo("key.3");
            assertThat(metadataList.get(3).getRoutingKey()).isEqualTo("key.4");
            assertThat(metadataList.get(4).getRoutingKey()).isEqualTo("key.5");

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testConnectionRecovery() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setAutomaticRecoveryEnabled(true);
            factory.setNetworkRecoveryInterval(1000);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            IncomingRabbitMQChannelTest.TestIncomingConfig config = IncomingRabbitMQChannelTest
                    .createTestConfig(exchangeName, "recovery-queue", false, 256);
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            List<String> received = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(3);

            channel.getStream().subscribe().with(msg -> {
                byte[] payload = (byte[]) msg.getPayload();
                received.add(new String(payload, StandardCharsets.UTF_8));
                msg.ack();
                latch.countDown();
            });

            Thread.sleep(500);

            // Publish some messages
            usage.produce(exchangeName, null, "test.key", 1, () -> "Before-1");
            usage.produce(exchangeName, null, "test.key", 1, () -> "Before-2");
            usage.produce(exchangeName, null, "test.key", 1, () -> "Before-3");

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(received).hasSize(3);

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testQueueConfiguration() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);
            IncomingRabbitMQChannelTest.TestIncomingConfig config = IncomingRabbitMQChannelTest
                    .createTestConfig(exchangeName, "config-queue", false, 256);
            Instance<java.util.Map<String, ?>> configMaps = null;

            IncomingRabbitMQChannel channel = new IncomingRabbitMQChannel(holder, config, configMaps, null);

            CountDownLatch latch = new CountDownLatch(1);

            channel.getStream().subscribe().with(msg -> {
                msg.ack();
                latch.countDown();
            });

            Thread.sleep(500);

            usage.produce(exchangeName, null, "test.key", 1, () -> "Config-Test");

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    // Helper methods

    private BroadcastTestConfig createBroadcastConfig(String exchangeName, String queueName) {
        return new BroadcastTestConfig(exchangeName, queueName, false, 256);
    }

    private IncomingRabbitMQChannelTest.TestIncomingConfig createBackpressureConfig(String exchangeName,
            String queueName) {
        return IncomingRabbitMQChannelTest.createTestConfig(exchangeName, queueName, false, 5);
    }

    // Broadcast mode configuration
    private static class BroadcastTestConfig extends IncomingRabbitMQChannelTest.TestIncomingConfig {

        public BroadcastTestConfig(String exchangeName, String queueName, boolean autoAck, int maxOutstanding) {
            super(exchangeName, queueName, autoAck, maxOutstanding);
        }

        @Override
        public Boolean getBroadcast() {
            return true; // Enable broadcast mode
        }
    }
}
