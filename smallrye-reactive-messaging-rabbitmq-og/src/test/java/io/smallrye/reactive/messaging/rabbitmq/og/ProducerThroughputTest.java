package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ProducerThroughputTest extends WeldTestBase {

    static final int MESSAGE_COUNT = 10_000;
    static final int SMALL_MESSAGE_COUNT = 1_000;

    private static final List<Result> results = new ArrayList<>();

    record Result(String category, String name, int count, long elapsedMs) {
        double throughput() {
            return elapsedMs > 0 ? (double) count / elapsedMs * 1000 : 0;
        }
    }

    private void record(String category, String name, int count, long elapsedMs) {
        results.add(new Result(category, name, count, elapsedMs));
    }

    @AfterAll
    static void printTable() {
        int nameWidth = results.stream().mapToInt(r -> r.name().length()).max().orElse(40);
        String fmt = "| %-" + nameWidth + "s | %,8d | %,6d ms | %,10.0f msg/s |%n";
        String header = String.format("| %-" + nameWidth + "s | %8s | %9s | %14s |",
                "Test", "Messages", "Time", "Throughput");
        String separator = "|" + "-".repeat(nameWidth + 2) + "|" + "-".repeat(10)
                + "|" + "-".repeat(11) + "|" + "-".repeat(16) + "|";

        System.out.println();
        System.out.println("=== Producer Throughput Results ===");

        String lastCategory = null;
        for (Result r : results) {
            if (!r.category().equals(lastCategory)) {
                System.out.println();
                System.out.println(r.category());
                System.out.println(separator);
                System.out.println(header);
                System.out.println(separator);
                lastCategory = r.category();
            }
            System.out.printf(fmt, r.name(), r.count(), r.elapsedMs(), r.throughput());
        }
        System.out.println(separator);
        System.out.println();
    }

    @Test
    @Order(0)
    void warmup() throws Exception {
        // Warm up raw client + broker
        ConnectionFactory factory = createConnectionFactory();
        String warmupExchange = "warmup-exchange";
        try (Connection connection = factory.newConnection();
                com.rabbitmq.client.Channel ch = connection.createChannel()) {
            ch.exchangeDeclare(warmupExchange, "topic", false, true, null);
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                ch.basicPublish(warmupExchange, "warmup", null, String.valueOf(i).getBytes());
            }
        }

        // Warm up reactive pipeline (Weld + connector + SenderProcessor)
        CountDownLatch received = new CountDownLatch(MESSAGE_COUNT);
        usage.consume(exchangeName, "test", v -> received.countDown());

        addBeans(OutgoingProducer.class);
        runApplication(outgoingConfig());

        assertThat(received.await(2, TimeUnit.MINUTES)).isTrue();
    }

    private MapBasedConfig outgoingConfig() {
        return commonConfig()
                .with("mp.messaging.outgoing.out.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.exchange.name", exchangeName)
                .with("mp.messaging.outgoing.out.exchange.declare", false)
                .with("mp.messaging.outgoing.out.default-routing-key", "test")
                .with("mp.messaging.outgoing.out.tracing.enabled", false);
    }

    // -- @Outgoing Multi --

    @Test
    @Order(1)
    void testOutgoingThroughput() throws InterruptedException {
        CountDownLatch received = new CountDownLatch(MESSAGE_COUNT);
        usage.consume(exchangeName, "test", v -> received.countDown());

        addBeans(OutgoingProducer.class);
        long start = System.nanoTime();
        runApplication(outgoingConfig());

        assertThat(received.await(2, TimeUnit.MINUTES)).isTrue();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        record("Connector — @Outgoing Multi", "inflight=1024 (default)", MESSAGE_COUNT, elapsed);
    }

    @Test
    @Order(2)
    void testOutgoingThroughputWithInflight10k() throws InterruptedException {
        CountDownLatch received = new CountDownLatch(MESSAGE_COUNT);
        usage.consume(exchangeName, "test", v -> received.countDown());

        addBeans(OutgoingProducer.class);
        long start = System.nanoTime();
        runApplication(outgoingConfig()
                .with("mp.messaging.outgoing.out.max-inflight-messages", MESSAGE_COUNT));

        assertThat(received.await(2, TimeUnit.MINUTES)).isTrue();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        record("Connector — @Outgoing Multi", "inflight=10000", MESSAGE_COUNT, elapsed);
    }

    @Test
    @Order(3)
    void testOutgoingThroughputWithInflightUnbounded() throws InterruptedException {
        CountDownLatch received = new CountDownLatch(MESSAGE_COUNT);
        usage.consume(exchangeName, "test", v -> received.countDown());

        addBeans(OutgoingProducer.class);
        long start = System.nanoTime();
        runApplication(outgoingConfig()
                .with("mp.messaging.outgoing.out.max-inflight-messages", -1));

        assertThat(received.await(2, TimeUnit.MINUTES)).isTrue();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        record("Connector — @Outgoing Multi", "inflight=unbounded", MESSAGE_COUNT, elapsed);
    }

    @Test
    @Order(4)
    void testOutgoingThroughputWithPublishConfirms() throws InterruptedException {
        CountDownLatch received = new CountDownLatch(MESSAGE_COUNT);
        usage.consume(exchangeName, "test", v -> received.countDown());

        addBeans(OutgoingProducer.class);
        long start = System.nanoTime();
        runApplication(outgoingConfig()
                .with("mp.messaging.outgoing.out.publish-confirms", true));

        assertThat(received.await(2, TimeUnit.MINUTES)).isTrue();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        record("Connector — @Outgoing Multi", "publish-confirms=true", MESSAGE_COUNT, elapsed);
    }

    @Test
    @Order(5)
    void testOutgoingThroughputWithInflight1() throws InterruptedException {
        CountDownLatch received = new CountDownLatch(SMALL_MESSAGE_COUNT);
        usage.consume(exchangeName, "test", v -> received.countDown());

        addBeans(OutgoingProducerSmall.class);
        long start = System.nanoTime();
        runApplication(outgoingConfig()
                .with("mp.messaging.outgoing.out.max-inflight-messages", 1));

        assertThat(received.await(2, TimeUnit.MINUTES)).isTrue();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        record("Connector — @Outgoing Multi", "inflight=1", SMALL_MESSAGE_COUNT, elapsed);
    }

    @Test
    @Order(6)
    void testOutgoingThroughputWithPublishConfirmsInflight1() throws InterruptedException {
        CountDownLatch received = new CountDownLatch(SMALL_MESSAGE_COUNT);
        usage.consume(exchangeName, "test", v -> received.countDown());

        addBeans(OutgoingProducerSmall.class);
        long start = System.nanoTime();
        runApplication(outgoingConfig()
                .with("mp.messaging.outgoing.out.publish-confirms", true)
                .with("mp.messaging.outgoing.out.max-inflight-messages", 1));

        assertThat(received.await(2, TimeUnit.MINUTES)).isTrue();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        record("Connector — @Outgoing Multi", "confirms + inflight=1", SMALL_MESSAGE_COUNT, elapsed);
    }

    // -- Emitter --

    @Test
    @Order(7)
    void testEmitterSendAndForget() throws InterruptedException {
        CountDownLatch received = new CountDownLatch(MESSAGE_COUNT);
        usage.consume(exchangeName, "test", v -> received.countDown());

        EmitterProducer producer = runApplication(outgoingConfig(), EmitterProducer.class);

        long start = System.nanoTime();
        CompletableFuture.runAsync(() -> producer.generate(MESSAGE_COUNT));

        assertThat(received.await(2, TimeUnit.MINUTES)).isTrue();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        record("Connector — Emitter", "sendAndForget (buffer=10k)", MESSAGE_COUNT, elapsed);
    }

    @Test
    @Order(8)
    void testEmitterSendAndForgetWithPublishConfirms() throws InterruptedException {
        CountDownLatch received = new CountDownLatch(MESSAGE_COUNT);
        usage.consume(exchangeName, "test", v -> received.countDown());

        EmitterProducer producer = runApplication(outgoingConfig()
                .with("mp.messaging.outgoing.out.publish-confirms", true), EmitterProducer.class);

        long start = System.nanoTime();
        CompletableFuture.runAsync(() -> producer.generate(MESSAGE_COUNT));

        assertThat(received.await(2, TimeUnit.MINUTES)).isTrue();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        record("Connector — Emitter", "sendAndForget + confirms", MESSAGE_COUNT, elapsed);
    }

    @Test
    @Order(9)
    void testEmitterSendAndForgetInflight10k() throws InterruptedException {
        CountDownLatch received = new CountDownLatch(MESSAGE_COUNT);
        usage.consume(exchangeName, "test", v -> received.countDown());

        EmitterProducer producer = runApplication(outgoingConfig()
                .with("mp.messaging.outgoing.out.max-inflight-messages", MESSAGE_COUNT), EmitterProducer.class);

        long start = System.nanoTime();
        CompletableFuture.runAsync(() -> producer.generate(MESSAGE_COUNT));

        assertThat(received.await(2, TimeUnit.MINUTES)).isTrue();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        record("Connector — Emitter", "sendAndForget inflight=10k", MESSAGE_COUNT, elapsed);
    }

    @Test
    @Order(10)
    void testEmitterSendAndForgetWithPublishConfirmsInflight10k() throws InterruptedException {
        CountDownLatch received = new CountDownLatch(MESSAGE_COUNT);
        usage.consume(exchangeName, "test", v -> received.countDown());

        EmitterProducer producer = runApplication(outgoingConfig()
                .with("mp.messaging.outgoing.out.publish-confirms", true)
                .with("mp.messaging.outgoing.out.max-inflight-messages", MESSAGE_COUNT), EmitterProducer.class);

        long start = System.nanoTime();
        CompletableFuture.runAsync(() -> producer.generate(MESSAGE_COUNT));

        assertThat(received.await(2, TimeUnit.MINUTES)).isTrue();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        record("Connector — Emitter", "sendAndForget + confirms inflight=10k", MESSAGE_COUNT, elapsed);
    }

    // -- Direct Client Baselines --

    @Test
    @Order(11)
    void testDirectClientNoConfirms() throws Exception {
        CountDownLatch received = new CountDownLatch(MESSAGE_COUNT);
        usage.consume(exchangeName, "test", v -> received.countDown());

        ConnectionFactory factory = createConnectionFactory();
        try (Connection connection = factory.newConnection();
                com.rabbitmq.client.Channel ch = connection.createChannel()) {

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                ch.basicPublish(exchangeName, "test", null, String.valueOf(i).getBytes());
            }
            assertThat(received.await(2, TimeUnit.MINUTES)).isTrue();
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            record("Direct Client Baselines", "no confirms", MESSAGE_COUNT, elapsed);
        }
    }

    @Test
    @Order(12)
    void testDirectClientPublishConfirmsBatch() throws Exception {
        CountDownLatch received = new CountDownLatch(MESSAGE_COUNT);
        usage.consume(exchangeName, "test", v -> received.countDown());

        ConnectionFactory factory = createConnectionFactory();
        try (Connection connection = factory.newConnection();
                com.rabbitmq.client.Channel ch = connection.createChannel()) {

            ch.confirmSelect();

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                ch.basicPublish(exchangeName, "test", null, String.valueOf(i).getBytes());
            }
            ch.waitForConfirmsOrDie(30000);
            assertThat(received.await(2, TimeUnit.MINUTES)).isTrue();
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            record("Direct Client Baselines", "publish-confirms (batch wait)", MESSAGE_COUNT, elapsed);
        }
    }

    @Test
    @Order(13)
    void testDirectClientConfirmPerMessage() throws Exception {
        CountDownLatch received = new CountDownLatch(MESSAGE_COUNT);
        usage.consume(exchangeName, "test", v -> received.countDown());

        ConnectionFactory factory = createConnectionFactory();
        try (Connection connection = factory.newConnection();
                com.rabbitmq.client.Channel ch = connection.createChannel()) {

            ch.confirmSelect();

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                ch.basicPublish(exchangeName, "test", null, String.valueOf(i).getBytes());
                ch.waitForConfirmsOrDie(5000);
            }
            assertThat(received.await(2, TimeUnit.MINUTES)).isTrue();
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            record("Direct Client Baselines", "confirm per message", MESSAGE_COUNT, elapsed);
        }
    }

    // -- Helpers --

    private ConnectionFactory createConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        return factory;
    }

    // -- Test Beans --

    @ApplicationScoped
    public static class OutgoingProducer {

        @Outgoing("out")
        public Multi<String> produce() {
            return Multi.createFrom().range(0, MESSAGE_COUNT).map(String::valueOf);
        }
    }

    @ApplicationScoped
    public static class OutgoingProducerSmall {

        @Outgoing("out")
        public Multi<String> produce() {
            return Multi.createFrom().range(0, SMALL_MESSAGE_COUNT).map(String::valueOf);
        }
    }

    @ApplicationScoped
    public static class EmitterProducer {

        @Inject
        @Channel("out")
        @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 10000)
        MutinyEmitter<String> emitter;

        public void generate(int count) {
            for (int i = 0; i < count; i++) {
                emitter.sendAndForget(String.valueOf(i));
            }
        }
    }
}
