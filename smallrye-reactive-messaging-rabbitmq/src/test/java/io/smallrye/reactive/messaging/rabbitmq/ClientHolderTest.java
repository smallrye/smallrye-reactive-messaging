package io.smallrye.reactive.messaging.rabbitmq;

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

import io.vertx.mutiny.core.Vertx;
import io.vertx.rabbitmq.RabbitMQOptions;

public class ClientHolderTest extends RabbitMQBrokerTestBase {

    private RabbitMQOptions clientOptions() {
        return new RabbitMQOptions()
                .setHost(host)
                .setPort(port)
                .setUser(username)
                .setPassword(password)
                .setAutomaticRecoveryOnInitialConnection(false)
                .setReconnectAttempts(5)
                .setReconnectInterval(1000);
    }

    @Test
    public void testConcurrentGetOrEstablishConnection() throws InterruptedException {
        Vertx vertx = executionHolder.vertx();
        io.vertx.mutiny.rabbitmq.RabbitMQClient client = io.vertx.mutiny.rabbitmq.RabbitMQClient
                .create(vertx, clientOptions());
        ClientHolder holder = new ClientHolder(client);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<io.vertx.mutiny.rabbitmq.RabbitMQClient> connections = new CopyOnWriteArrayList<>();
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    io.vertx.mutiny.rabbitmq.RabbitMQClient conn = holder.getOrEstablishConnection()
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

        io.vertx.mutiny.rabbitmq.RabbitMQClient first = connections.get(0);
        assertThat(connections).allSatisfy(conn -> assertThat(conn).isSameAs(first));
        assertThat(client.isConnected()).isTrue();

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        client.stop().await().atMost(Duration.ofSeconds(5));
    }

    @Test
    public void testReconnectAfterDisconnection() {
        Vertx vertx = executionHolder.vertx();
        io.vertx.mutiny.rabbitmq.RabbitMQClient client = io.vertx.mutiny.rabbitmq.RabbitMQClient
                .create(vertx, clientOptions());
        ClientHolder holder = new ClientHolder(client);

        io.vertx.mutiny.rabbitmq.RabbitMQClient first = holder.getOrEstablishConnection()
                .await().atMost(Duration.ofSeconds(10));
        assertThat(first).isNotNull();
        assertThat(client.isConnected()).isTrue();
        assertThat(holder.hasBeenConnected()).isTrue();

        io.vertx.mutiny.rabbitmq.RabbitMQClient cached = holder.getOrEstablishConnection()
                .await().atMost(Duration.ofSeconds(10));
        assertThat(cached).isSameAs(first);

        client.stop().await().atMost(Duration.ofSeconds(5));
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(client.isConnected()).isFalse());

        io.vertx.mutiny.rabbitmq.RabbitMQClient reconnected = holder.getOrEstablishConnection()
                .await().atMost(Duration.ofSeconds(10));
        assertThat(reconnected).isSameAs(first);
        assertThat(client.isConnected()).isTrue();
    }

    @Test
    public void testConcurrentGetOrEstablishConnectionAfterDisconnect() throws InterruptedException {
        Vertx vertx = executionHolder.vertx();
        io.vertx.mutiny.rabbitmq.RabbitMQClient client = io.vertx.mutiny.rabbitmq.RabbitMQClient
                .create(vertx, clientOptions());
        ClientHolder holder = new ClientHolder(client);

        holder.getOrEstablishConnection().await().atMost(Duration.ofSeconds(10));
        assertThat(client.isConnected()).isTrue();

        client.stop().await().atMost(Duration.ofSeconds(5));
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(client.isConnected()).isFalse());

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<io.vertx.mutiny.rabbitmq.RabbitMQClient> connections = new CopyOnWriteArrayList<>();
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    io.vertx.mutiny.rabbitmq.RabbitMQClient conn = holder.getOrEstablishConnection()
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

        assertThat(connections).allSatisfy(conn -> assertThat(conn).isSameAs(client));
        assertThat(client.isConnected()).isTrue();

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        client.stop().await().atMost(Duration.ofSeconds(5));
    }
}
