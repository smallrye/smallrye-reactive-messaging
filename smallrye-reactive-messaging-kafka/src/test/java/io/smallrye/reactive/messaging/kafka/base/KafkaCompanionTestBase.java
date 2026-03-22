package io.smallrye.reactive.messaging.kafka.base;

import java.lang.reflect.Method;
import java.util.UUID;

import jakarta.enterprise.inject.Instance;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.KafkaCDIEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.test.KafkaBrokerExtension;
import io.smallrye.reactive.messaging.kafka.companion.test.KafkaBrokerExtension.KafkaBootstrapServers;
import io.smallrye.reactive.messaging.kafka.impl.KafkaShareGroupSource;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

@ExtendWith(KafkaBrokerExtension.class)
public class KafkaCompanionTestBase extends WeldTestBase {

    public static final int KAFKA_PORT = 9092;

    public Vertx vertx;
    public static KafkaCompanion companion;

    public String topic;

    @BeforeAll
    static void initCompanion(@KafkaBootstrapServers String bootstrapServers) {
        companion = new KafkaCompanion(bootstrapServers);
        companion.registerSerde(JsonObject.class,
                Serdes.serdeFrom(new JsonObjectSerde.JsonObjectSerializer(), new JsonObjectSerde.JsonObjectDeserializer()));
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
    static void closeCompanion() {
        companion.close();
    }

    public KafkaMapBasedConfig kafkaConfig() {
        return kafkaConfig("");
    }

    public KafkaMapBasedConfig kafkaConfig(String prefix) {
        return kafkaConfig(prefix, false);
    }

    public KafkaMapBasedConfig kafkaConfig(String prefix, boolean tracing) {
        return new KafkaMapBasedConfig(prefix, tracing).put("bootstrap.servers", companion.getBootstrapServers());
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

    public int getMaxNumberOfEventLoop(int expected) {
        // On Github Actions, only one event loop is created.
        int cpus = CpuCoreSensor.availableProcessors();
        // For some reason when Github Actions has 4 cores it'll still run on 1 event loop thread
        if (cpus <= 4) {
            return 1;
        }
        return Math.min(expected, cpus / 2);
    }

    public <K, V> KafkaSource<K, V> createSource(MapBasedConfig config) {
        return createSource(UUID.randomUUID().toString(), config);
    }

    public <K, V> KafkaSource<K, V> createSource(String consumerGroup, MapBasedConfig config) {
        return createSource(consumerGroup, config, -1);
    }

    public <K, V> KafkaSource<K, V> createSource(String consumerGroup, MapBasedConfig config, int index) {
        return createSource(consumerGroup, config,
                UnsatisfiedInstance.instance(), index);
    }

    public <K, V> KafkaSource<K, V> createSource(String consumerGroup, MapBasedConfig config,
            Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers,
            int index) {
        return createSource(consumerGroup, config, UnsatisfiedInstance.instance(),
                deserializationFailureHandlers, index);
    }

    public <K, V> KafkaSource<K, V> createSource(String consumerGroup, MapBasedConfig config,
            Instance<KafkaConsumerRebalanceListener> rebalanceListeners,
            Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers,
            int index) {
        return new KafkaSource<>(vertx, consumerGroup, new KafkaConnectorIncomingConfiguration(config),
                UnsatisfiedInstance.instance(), commitHandlerFactories, failureHandlerFactories,
                rebalanceListeners, CountKafkaCdiEvents.noCdiEvents, getAdminClientRegistry(),
                UnsatisfiedInstance.instance(), deserializationFailureHandlers, index);
    }

    public <K, V> KafkaShareGroupSource<K, V> createShareGroupSource(String shareGroup, MapBasedConfig config) {
        return new KafkaShareGroupSource<>(vertx, shareGroup, new KafkaConnectorIncomingConfiguration(config),
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, getAdminClientRegistry(),
                UnsatisfiedInstance.instance(), UnsatisfiedInstance.instance());
    }

    public KafkaSink createSink(MapBasedConfig config) {
        return createSink(config, CountKafkaCdiEvents.noCdiEvents);
    }

    public KafkaSink createSink(MapBasedConfig config, KafkaCDIEvents cdiEvents) {
        return new KafkaSink(new KafkaConnectorOutgoingConfiguration(config), cdiEvents, getAdminClientRegistry(),
                UnsatisfiedInstance.instance(), UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(), UnsatisfiedInstance.instance());
    }

}
