package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;

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
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionProxyTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;

public class NoKafkaTest extends KafkaCompanionProxyTestBase {

    @BeforeEach
    void setUp() {
        disableProxy();
    }

    @Test
    public void testOutgoingWithoutKafkaCluster() {
        runApplication(myKafkaSinkConfigWithoutBlockLimit(topic), MyOutgoingBean.class);

        await().until(() -> {
            HealthReport readiness = getHealth().getReadiness();
            return !readiness.isOk();
        });

        await().until(() -> {
            // liveness is ok, as we don't check the connection with the broker
            HealthReport liveness = getHealth().getLiveness();
            return liveness.isOk();
        });

        enableProxy();

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(companion.consumeStrings().fromTopics(topic, 3, Duration.ofMinutes(3)).awaitCompletion()
                .count()).isEqualTo(3);
    }

    @Test
    public void testIncomingWithoutKafkaCluster() {
        MyIncomingBean bean = runApplication(myKafkaSourceConfig(), MyIncomingBean.class);
        assertThat(bean.received()).hasSize(0);

        await().until(() -> !isReady());
        await().until(this::isAlive);

        enableProxy();

        await().until(this::isReady);
        await().until(this::isAlive);

        AtomicInteger counter = new AtomicInteger();
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "1", i), 5);

        // Wait a bit longer as we may not have a leader for the topic yet.
        await()
                .atMost(1, TimeUnit.MINUTES)
                .until(() -> bean.received().size() == 5);

    }

    @Test
    public void testIncomingWithoutKafkaClusterUsingAdminHealthCheck() {
        MyIncomingBean bean = runApplication(myKafkaSourceConfig()
                .with("health-topic-verification-enabled", true),
                MyIncomingBean.class);
        assertThat(bean.received()).hasSize(0);

        await().until(() -> !isReady());
        await().until(this::isAlive);

        enableProxy();

        await().until(this::isReady);
        await().until(this::isAlive);

        AtomicInteger counter = new AtomicInteger();
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "1", i), 5);

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
        // Depending on whether the subscription has been received or not, we may have caught a failure or not.
        Throwable throwable = bean.failure();
        if (throwable != null) {
            assertThat(throwable).isInstanceOf(BackPressureFailure.class);
        }
        bean.stop();
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
        private volatile boolean stopped = false;

        public Throwable failure() {
            return failure.get();
        }

        @Outgoing("temperature-values")
        public Multi<String> generate() {
            return Multi.createFrom().ticks().every(Duration.ofMillis(200))
                    .map(l -> Long.toString(l))
                    .onFailure().invoke(failure::set)
                    .select().where(s -> !stopped);
        }

        public void stop() {
            stopped = true;
        }
    }

    private KafkaMapBasedConfig myKafkaSourceConfig() {
        return kafkaConfig("mp.messaging.incoming.temperature-values")
                .build(
                        "value.deserializer", IntegerDeserializer.class.getName(),
                        "topic", topic,
                        "auto.offset.reset", "earliest");
    }

    private KafkaMapBasedConfig myKafkaSinkConfig(String topic) {
        return kafkaConfig("mp.messaging.outgoing.temperature-values")
                .build(
                        "value.serializer", StringSerializer.class.getName(),
                        "max-inflight-messages", "2",
                        "max.block.ms", 1000,
                        "topic", topic,

                        // Speed up kafka admin failure
                        "default.api.timeout.ms", 250,
                        "request.timeout.ms", 200);
    }

    private KafkaMapBasedConfig myKafkaSinkConfigWithoutBlockLimit(String topic) {
        return kafkaConfig("mp.messaging.outgoing.temperature-values")
                .build(
                        "value.serializer", StringSerializer.class.getName(),
                        "topic", topic);
    }

}
