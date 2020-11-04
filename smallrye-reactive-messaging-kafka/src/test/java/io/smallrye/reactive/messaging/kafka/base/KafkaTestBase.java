package io.smallrye.reactive.messaging.kafka.base;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

import javax.enterprise.inject.Instance;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.strimzi.StrimziKafkaContainer;
import io.vertx.mutiny.core.Vertx;

public class KafkaTestBase extends WeldTestBase {
    public static final int KAFKA_PORT = 9092;
    public static StrimziKafkaContainer kafka;

    public Vertx vertx;
    public KafkaUsage usage;

    // A random topic.
    public String topic;

    @BeforeAll
    public static void startKafkaBroker() {
        kafka = new StrimziKafkaContainer();
        kafka.start();
    }

    @AfterAll
    public static void stopKafkaBroker() {
        if (kafka != null) {
            try {
                kafka.stop();
            } catch (Exception e) {
                // Ignore it.
            }
            await().until(() -> !kafka.isRunning());
        }
    }

    @BeforeEach
    public void createVertxAndInitUsage() {
        vertx = Vertx.vertx();
        topic = UUID.randomUUID().toString();
        usage = new KafkaUsage();
    }

    @AfterEach
    public void closeVertx() {
        if (vertx != null) {
            vertx.closeAndAwait();
        }
    }

    public MapBasedConfig newCommonConfigForSource() {
        String randomId = UUID.randomUUID().toString();
        return MapBasedConfig.builder().put(
                "bootstrap.servers", getBootstrapServers(),
                "group.id", randomId,
                "key.deserializer", StringDeserializer.class.getName(),
                "enable.auto.commit", "false",
                "auto.offset.reset", "earliest",
                "tracing-enabled", false,
                "topic", topic,
                "channel-name", topic).build();
    }

    public String getBootstrapServers() {
        return kafka.getBootstrapServers();
    }

    public void createTopic(String topic, int partition) {
        try (AdminClient admin = AdminClient.create(
                Collections.singletonMap(BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers()))) {
            admin.createTopics(Collections.singletonList(new NewTopic(topic, partition, (short) 1)));
        }
    }

    /**
     * We need to restart the broker on the same exposed port.
     * Test Containers makes this unnecessarily complicated, but well, let's go for a hack.
     * See https://github.com/testcontainers/testcontainers-java/issues/256.
     *
     * @param gracePeriodInSecond number of seconds to wait before restarting
     */
    public void restart(int gracePeriodInSecond) {
        int port = getKafkaPort();
        try {
            kafka.stop();
        } catch (Exception e) {
            // Ignore me.
        }
        await().until(() -> !kafka.isRunning());
        sleep(Duration.ofSeconds(gracePeriodInSecond));

        startKafkaBroker(port);
    }

    public static void startKafkaBroker(int port) {
        if (kafka != null && kafka.isRunning()) {
            kafka.stop();
            await().until(() -> !kafka.isRunning());
        }
        kafka = new FixedKafkaContainer(port);
        kafka.start();
        await().until(() -> kafka.isRunning());
    }

    public int getKafkaPort() {
        return kafka.getMappedPort(KAFKA_PORT);
    }

    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public Instance<KafkaConsumerRebalanceListener> getConsumerRebalanceListeners() {
        return getBeanManager().createInstance().select(KafkaConsumerRebalanceListener.class);
    }

    /**
     * Specialization of {@link StrimziKafkaContainer} but exposing the Kafka port to a specific host port.
     * Useful when you need to restart Kafka on the same port.
     */
    public static class FixedKafkaContainer extends StrimziKafkaContainer {
        public FixedKafkaContainer(int port) {
            super();
            super.addFixedExposedPort(port, KAFKA_PORT);
        }

    }

}
