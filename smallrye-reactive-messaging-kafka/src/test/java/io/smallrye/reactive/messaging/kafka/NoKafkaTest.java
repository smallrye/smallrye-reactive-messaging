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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.BackPressureFailure;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.base.MapBasedConfig;

public class NoKafkaTest extends KafkaTestBase {

    private static int port;

    /**
     * Prepare the tests:
     * - get the Kafka port - so we know it's free, and store it
     * - stop Kafka
     */
    @BeforeEach
    public void prepare() {
        if (kafka.isRunning()) {
            port = getKafkaPort();
            stopKafkaBroker();
        }
    }

    @Test
    public void testOutgoingWithoutKafkaCluster() throws InterruptedException {
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

        startKafkaBroker(port);

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
        MyIncomingBean bean = runApplication(myKafkaSourceConfig(), MyIncomingBean.class);
        assertThat(bean.received()).hasSize(0);

        await().until(() -> !isReady());
        await().until(this::isAlive);

        startKafkaBroker(port);

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
        await().until(() -> {
            // Failure caught
            return !isAlive();
        });

        Throwable throwable = bean.failure();
        assertThat(throwable).isNotNull();
        assertThat(throwable).isInstanceOf(BackPressureFailure.class);
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

        public Throwable failure() {
            return failure.get();
        }

        @Outgoing("temperature-values")
        public Multi<String> generate() {
            return Multi.createFrom().ticks().every(Duration.ofMillis(200))
                    // No overflow management - we want it to fail.
                    .map(l -> Long.toString(l))
                    .onFailure().invoke(failure::set);
        }
    }

    private MapBasedConfig myKafkaSourceConfig() {
        return MapBasedConfig.builder("mp.messaging.incoming.temperature-values")
                .put(
                        "value.deserializer", IntegerDeserializer.class.getName(),
                        "topic", topic,
                        "auto.offset.reset", "earliest")
                .build();
    }

    private MapBasedConfig myKafkaSinkConfig(String topic) {
        return MapBasedConfig.builder("mp.messaging.outgoing.temperature-values")
                .put(
                        "value.serializer", StringSerializer.class.getName(),
                        "max-inflight-messages", "2",
                        "max.block.ms", 1000,
                        "topic", topic)
                .build();
    }

    private MapBasedConfig myKafkaSinkConfigWithoutBlockLimit(String topic) {
        return MapBasedConfig.builder("mp.messaging.outgoing.temperature-values")
                .put(
                        "value.serializer", StringSerializer.class.getName(),
                        "topic", topic)
                .build();
    }

}
