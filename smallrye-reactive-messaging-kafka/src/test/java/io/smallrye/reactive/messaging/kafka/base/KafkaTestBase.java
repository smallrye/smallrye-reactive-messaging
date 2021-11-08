package io.smallrye.reactive.messaging.kafka.base;

import java.lang.reflect.Method;
import java.util.UUID;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import io.smallrye.reactive.messaging.kafka.base.KafkaBrokerExtension.KafkaBootstrapServers;
import io.vertx.mutiny.core.Vertx;

@ExtendWith(KafkaBrokerExtension.class)
public class KafkaTestBase extends WeldTestBase {
    public static final int KAFKA_PORT = 9092;

    public Vertx vertx;
    public static KafkaUsage usage;

    public String topic;

    @BeforeAll
    static void initUsage(@KafkaBootstrapServers String bootstrapServers) {
        usage = new KafkaUsage(bootstrapServers);
    }

    @BeforeEach
    public void createVertxAndInitUsage() {
        vertx = Vertx.vertx();
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

    @AfterAll
    static void closeUsage() {
        usage.close();
    }

    public KafkaMapBasedConfig kafkaConfig() {
        return kafkaConfig("");
    }

    public KafkaMapBasedConfig kafkaConfig(String prefix) {
        return kafkaConfig(prefix, false);
    }

    public KafkaMapBasedConfig kafkaConfig(String prefix, boolean tracing) {
        return new KafkaMapBasedConfig(prefix, tracing).put("bootstrap.servers", usage.getBootstrapServers());
    }

    public KafkaMapBasedConfig newCommonConfigForSource() {
        String randomId = UUID.randomUUID().toString();
        return kafkaConfig().build(
                "group.id", randomId,
                "key.deserializer", StringDeserializer.class.getName(),
                "enable.auto.commit", "false",
                "auto.offset.reset", "earliest",
                "tracing-enabled", false,
                "topic", topic,
                "graceful-shutdown", false,
                "channel-name", topic);
    }

}
