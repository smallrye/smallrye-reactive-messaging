package io.smallrye.reactive.messaging.kafka.base;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import io.strimzi.StrimziKafkaContainer;
import io.vertx.mutiny.core.Vertx;

@ExtendWith(KafkaBrokerExtension.class)
public class KafkaTestBase extends WeldTestBase {
    public static final int KAFKA_PORT = 9092;

    public Vertx vertx;
    public KafkaUsage usage;

    public String topic;

    @BeforeEach
    public void createVertxAndInitUsage() {
        vertx = Vertx.vertx();
        usage = new KafkaUsage();
    }

    @BeforeEach
    public void initTopic(TestInfo testInfo) {
        String cn = testInfo.getTestClass().map(Class::getSimpleName).orElse(UUID.randomUUID().toString());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        topic = cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    @AfterEach
    public void closeVertx() {
        if (vertx != null) {
            vertx.closeAndAwait();
        }
    }

    public KafkaMapBasedConfig newCommonConfigForSource() {
        String randomId = UUID.randomUUID().toString();
        return KafkaMapBasedConfig.builder().put(
                "bootstrap.servers", getBootstrapServers(),
                "group.id", randomId,
                "key.deserializer", StringDeserializer.class.getName(),
                "enable.auto.commit", "false",
                "auto.offset.reset", "earliest",
                "tracing-enabled", false,
                "topic", topic,
                "graceful-shutdown", false,
                "channel-name", topic).build();
    }

    public static String getBootstrapServers() {
        return KafkaBrokerExtension.getBootstrapServers();
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
     * @param kafka the broker that will be closed
     * @param gracePeriodInSecond number of seconds to wait before restarting
     * @return the new broker
     */
    public FixedKafkaContainer restart(StrimziKafkaContainer kafka, int gracePeriodInSecond) {
        int port = kafka.getMappedPort(KAFKA_PORT);
        try {
            kafka.close();
        } catch (Exception e) {
            // Ignore me.
        }
        await().until(() -> !kafka.isRunning());
        sleep(Duration.ofSeconds(gracePeriodInSecond));

        return startKafkaBroker(port);
    }

    public static FixedKafkaContainer startKafkaBroker(int port) {
        FixedKafkaContainer kafka = new FixedKafkaContainer(port);
        kafka.start();
        await().until(kafka::isRunning);
        return kafka;
    }

    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Specialization of {@link StrimziKafkaContainer} but exposing the Kafka port to a specific host port.
     * Useful when you need to restart Kafka on the same port.
     */
    public static class FixedKafkaContainer extends StrimziKafkaContainer {
        public FixedKafkaContainer(int port) {
            super(KafkaBrokerExtension.KAFKA_VERSION);
            super.addFixedExposedPort(port, KAFKA_PORT);
        }

    }

    public static String getHeader(Headers headers, String key) {
        return new String(headers.lastHeader(key).value(), StandardCharsets.UTF_8);
    }

}
