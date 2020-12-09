package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.BackPressureFailure;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.strimzi.StrimziKafkaContainer;

public class NoKafkaTest extends KafkaTestBase {

    private static int port;
    private static String servers;

    private GenericContainer<?> kafka;

    @BeforeAll
    public static void getFreePort() {
        StrimziKafkaContainer kafka = new StrimziKafkaContainer();
        kafka.start();
        await().until(kafka::isRunning);
        servers = kafka.getBootstrapServers();
        port = kafka.getMappedPort(KAFKA_PORT);
        kafka.close();
        await().until(() -> !kafka.isRunning());
    }

    @AfterEach
    public void close() {
        if (kafka != null) {
            kafka.close();
        }
    }

    @Test
    public void testOutgoingWithoutKafkaCluster() throws InterruptedException {
        usage.setBootstrapServers(servers);
        List<Map.Entry<String, String>> received = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);

        runApplication(myKafkaSinkConfigWithoutBlockLimit(topic), MyOutgoingBean.class);

        assertThat(expected).hasValue(0);

        await().until(() -> {
            HealthReport readiness = getHealth().getReadiness();
            return !readiness.isOk();
        });

        await().until(() -> {
            // liveness is ok, as we don't check the connection with the broker
            HealthReport liveness = getHealth().getLiveness();
            return liveness.isOk();
        });

        kafka = startKafkaBroker(port);

        await().until(this::isReady);
        await().until(this::isAlive);

        usage.consumeStrings(topic, 3, 1, TimeUnit.MINUTES,
                latch::countDown,
                (k, v) -> {
                    received.add(entry(k, v));
                    expected.getAndIncrement();
                });

        await().until(() -> received.size() == 3);

        latch.await();
    }

    @Test
    public void testIncomingWithoutKafkaCluster() {
        usage.setBootstrapServers(servers);
        MyIncomingBean bean = runApplication(myKafkaSourceConfig(), MyIncomingBean.class);
        assertThat(bean.received()).hasSize(0);

        await().until(() -> !isReady());
        await().until(this::isAlive);

        kafka = startKafkaBroker(port);

        await().until(this::isReady);
        await().until(this::isAlive);

        AtomicInteger counter = new AtomicInteger();
        usage.produceIntegers(5, null, () -> new ProducerRecord<>(topic, "1", counter.getAndIncrement()));

        // Wait a bit longer as we may not have a leader for the topic yet.
        await()
                .atMost(1, TimeUnit.MINUTES)
                .until(() -> bean.received().size() == 5);

    }

    @Test
    public void testOutgoingWithoutKafkaClusterWithoutBackPressure() {
        MyOutgoingBeanWithoutBackPressure bean = runApplication(myKafkaSinkConfig(topic),
                MyOutgoingBeanWithoutBackPressure.class);
        await().until(() -> !isReady());
        // Depending if the subscription has been achieve we may have caught a failure or not.
        Throwable throwable = bean.failure();
        if (throwable != null) {
            assertThat(throwable).isInstanceOf(BackPressureFailure.class);
        }
    }

    @ApplicationScoped
    public static class MyOutgoingBean {

        @Outgoing("temperature-values")
        public Multi<String> generate() {
            return Multi.createFrom().emitter(e -> {
                e.emit("0");
                e.emit("1");
                e.emit("2");
                e.complete();
            });
        }
    }

    @ApplicationScoped
    public static class MyIncomingBean {

        final List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("temperature-values")
        public void consume(int p) {
            received.add(p);
        }

        public List<Integer> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class MyOutgoingBeanWithoutBackPressure {

        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicBoolean subscribed = new AtomicBoolean();

        public Throwable failure() {
            return failure.get();
        }

        @Outgoing("temperature-values")
        public Multi<String> generate() {
            return Multi.createFrom().ticks().every(Duration.ofMillis(200))
                    // No overflow management - we want it to fail.
                    .onSubscribe().invoke(() -> subscribed.set(true))
                    .map(l -> Long.toString(l))
                    .onFailure().invoke(failure::set);
        }
    }

    private KafkaMapBasedConfig myKafkaSourceConfig() {
        return KafkaMapBasedConfig.builder("mp.messaging.incoming.temperature-values")
                .put(
                        "value.deserializer", IntegerDeserializer.class.getName(),
                        "topic", topic,
                        "bootstrap.servers", servers,
                        "auto.offset.reset", "earliest")
                .build();
    }

    private KafkaMapBasedConfig myKafkaSinkConfig(String topic) {
        return KafkaMapBasedConfig.builder("mp.messaging.outgoing.temperature-values")
                .put(
                        "value.serializer", StringSerializer.class.getName(),
                        "max-inflight-messages", "2",
                        "max.block.ms", 1000,
                        "topic", topic,
                        "bootstrap.servers", servers)
                .build();
    }

    private KafkaMapBasedConfig myKafkaSinkConfigWithoutBlockLimit(String topic) {
        return KafkaMapBasedConfig.builder("mp.messaging.outgoing.temperature-values")
                .put(
                        "value.serializer", StringSerializer.class.getName(),
                        "topic", topic,
                        "bootstrap.servers", servers)
                .build();
    }

}
