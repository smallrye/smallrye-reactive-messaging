package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.amqp.AmqpConnection;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public class ConnectionHolderTest extends AmqpBrokerTestBase {

    @Test
    public void testConcurrentGetOrEstablishConnection() throws InterruptedException {
        Vertx vertx = executionHolder.vertx();
        AmqpClient client = AmqpClient.create(vertx, new AmqpClientOptions()
                .setHost(host)
                .setPort(port)
                .setUsername(username)
                .setPassword(password));
        Context rootContext = Context.newInstance(vertx.getDelegate().getOrCreateContext());

        MapBasedConfig config = new MapBasedConfig()
                .with("channel-name", "test-concurrent")
                .with("reconnect-attempts", 5)
                .with("reconnect-interval", 1)
                .with("health-timeout", 3);
        AmqpConnectorCommonConfiguration configuration = new AmqpConnectorCommonConfiguration(config);

        ConnectionHolder holder = new ConnectionHolder(client, null, configuration, vertx, rootContext);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<AmqpConnection> connections = new CopyOnWriteArrayList<>();
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    AmqpConnection conn = holder.getOrEstablishConnection()
                            .await().atMost(Duration.ofSeconds(10));
                    connections.add(conn);
                } catch (Throwable t) {
                    errors.add(t);
                }
            });
        }

        startLatch.countDown();

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(connections).hasSize(threadCount));
        assertThat(errors).isEmpty();

        AmqpConnection first = connections.get(0);
        assertThat(connections).allSatisfy(conn -> assertThat(conn).isSameAs(first));

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        client.close().await().atMost(Duration.ofSeconds(5));
    }

    @Test
    public void testReconnectAfterDisconnection() {
        Vertx vertx = executionHolder.vertx();
        AmqpClient client = AmqpClient.create(vertx, new AmqpClientOptions()
                .setHost(host)
                .setPort(port)
                .setUsername(username)
                .setPassword(password));
        Context rootContext = Context.newInstance(vertx.getDelegate().getOrCreateContext());

        MapBasedConfig config = new MapBasedConfig()
                .with("channel-name", "test-reconnect")
                .with("reconnect-attempts", 5)
                .with("reconnect-interval", 1)
                .with("health-timeout", 3);
        AmqpConnectorCommonConfiguration configuration = new AmqpConnectorCommonConfiguration(config);

        ConnectionHolder holder = new ConnectionHolder(client, null, configuration, vertx, rootContext);

        AmqpConnection first = holder.getOrEstablishConnection()
                .await().atMost(Duration.ofSeconds(10));
        assertThat(first).isNotNull();
        assertThat(first.isDisconnected()).isFalse();

        AmqpConnection cached = holder.getOrEstablishConnection()
                .await().atMost(Duration.ofSeconds(10));
        assertThat(cached).isSameAs(first);

        first.close().await().atMost(Duration.ofSeconds(5));
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(first.isDisconnected()).isTrue());

        AmqpConnection reconnected = holder.getOrEstablishConnection()
                .await().atMost(Duration.ofSeconds(10));
        assertThat(reconnected).isNotNull();
        assertThat(reconnected).isNotSameAs(first);
        assertThat(reconnected.isDisconnected()).isFalse();

        reconnected.close().await().atMost(Duration.ofSeconds(5));
        client.close().await().atMost(Duration.ofSeconds(5));
    }

    @Test
    public void testConcurrentGetOrEstablishConnectionAfterDisconnect() throws InterruptedException {
        Vertx vertx = executionHolder.vertx();
        AmqpClient client = AmqpClient.create(vertx, new AmqpClientOptions()
                .setHost(host)
                .setPort(port)
                .setUsername(username)
                .setPassword(password));
        Context rootContext = Context.newInstance(vertx.getDelegate().getOrCreateContext());

        MapBasedConfig config = new MapBasedConfig()
                .with("channel-name", "test-concurrent-reconnect")
                .with("reconnect-attempts", 5)
                .with("reconnect-interval", 1)
                .with("health-timeout", 3);
        AmqpConnectorCommonConfiguration configuration = new AmqpConnectorCommonConfiguration(config);

        ConnectionHolder holder = new ConnectionHolder(client, null, configuration, vertx, rootContext);

        AmqpConnection first = holder.getOrEstablishConnection()
                .await().atMost(Duration.ofSeconds(10));
        assertThat(first).isNotNull();

        first.close().await().atMost(Duration.ofSeconds(5));
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(first.isDisconnected()).isTrue());

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<AmqpConnection> connections = new CopyOnWriteArrayList<>();
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    AmqpConnection conn = holder.getOrEstablishConnection()
                            .await().atMost(Duration.ofSeconds(10));
                    connections.add(conn);
                } catch (Throwable t) {
                    errors.add(t);
                }
            });
        }

        startLatch.countDown();

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(connections).hasSize(threadCount));
        assertThat(errors).isEmpty();

        AmqpConnection reconnected = connections.get(0);
        assertThat(reconnected).isNotSameAs(first);
        assertThat(connections).allSatisfy(conn -> assertThat(conn).isSameAs(reconnected));

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        client.close().await().atMost(Duration.ofSeconds(5));
    }
}
