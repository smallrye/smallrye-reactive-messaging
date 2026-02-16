package io.smallrye.reactive.messaging.kafka.companion.test;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Node;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.extension.*;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.shaded.org.awaitility.core.ConditionTimeoutException;

import io.strimzi.test.container.StrimziKafkaContainer;

/**
 * Junit extension for creating Kafka broker
 */
public class KafkaBrokerExtension implements BeforeAllCallback, BeforeEachCallback, ParameterResolver, CloseableResource {
    public static final Logger LOGGER = Logger.getLogger(KafkaBrokerExtension.class.getName());

    public static final String KAFKA_IMAGE = "apache/kafka";
    public static final String KAFKA_VERSION = "4.2.0";
    public static final String STRIMZI_VERSION = "4.1.0";

    protected GenericContainer<?> kafka;

    @Override
    public void beforeAll(ExtensionContext context) {
        ExtensionContext.Store globalStore = context.getRoot().getStore(GLOBAL);
        KafkaBrokerExtension extension = (KafkaBrokerExtension) globalStore.get(KafkaBrokerExtension.class);
        if (extension == null) {
            LOGGER.info("Starting Kafka broker");
            startKafkaBroker();
            globalStore.put(KafkaBrokerExtension.class, this);
        }
    }

    @Override
    public void close() {
        LOGGER.info("Stopping Kafka broker");
        stopKafkaBroker();
    }

    public static GenericContainer<?> createKafkaContainer() {
        String kafkaImage = System.getProperty("kafka-container-image", KAFKA_IMAGE);
        String kafkaVersion = System.getProperty("kafka-container-version", KAFKA_VERSION);
        String strimziVersion = System.getProperty("strimzi-container-version", STRIMZI_VERSION);
        GenericContainer<? extends GenericContainer<?>> kafka;
        if (kafkaImage.contains("strimzi")) {
            kafka = new StrimziKafkaContainer(kafkaImage + ":latest-kafka-" + strimziVersion);
        } else {
            kafka = new KafkaContainer(kafkaImage + ":" + kafkaVersion);
        }
        return configureContainer(kafka);
    }

    public static <T extends GenericContainer<?>> GenericContainer<?> configureContainer(T container) {
        if (container instanceof StrimziKafkaContainer s) {
            return configureStrimziContainer(s);
        } else if (container instanceof KafkaContainer k) {
            return configureKafkaContainer(k);
        } else {
            return container;
        }
    }

    private static KafkaContainer configureKafkaContainer(KafkaContainer container) {
        Map<String, String> envVars = Map.of(
                "KAFKA_LOG_CLEANER_ENABLE", "false",
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0",
                "KAFKA_UNSTABLE_API_VERSIONS_ENABLE", "true",
                "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS", "classic,consumer,share",
                "KAFKA_GROUP_SHARE_ENABLE", "true",
                //                "KAFKA_SHARE_COORDINATOR_APPEND_LINGER_MS", "-1",
                // a single node topic needs to have 1 as replication factor
                "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1");
        return container.withEnv(envVars);
    }

    public static StrimziKafkaContainer configureStrimziContainer(StrimziKafkaContainer container) {
        String strimziVersion = System.getProperty("strimzi-container-version",
                System.getProperty("kafka-container-version", STRIMZI_VERSION));
        container.withKafkaVersion(strimziVersion);
        Map<String, String> config = new HashMap<>();
        config.put("log.cleaner.enable", "false");
        config.put("group.initial.rebalance.delay.ms", "0");
        config.put("unstable.api.versions.enable", "true");
        config.put("group.coordinator.rebalance.protocols", "classic,consumer,share");
        config.put("group.share.enable", "true");
        // a single node topic needs to have 1 as replication factor
        config.put("share.coordinator.state.topic.replication.factor", "1");
        container.withKafkaConfigurationMap(config);
        return container;
    }

    public void startKafkaBroker() {
        kafka = createKafkaContainer();
        kafka.start();
        LOGGER.info("Kafka broker started: " + getBootstrapServers(kafka) + " (" + kafka.getMappedPort(9092) + ")");
        await().until(() -> kafka.isRunning());
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
    public static GenericContainer<?> restart(GenericContainer<?> kafka, int gracePeriodInSecond) {
        int port = kafka.getMappedPort(9092);
        try {
            kafka.close();
        } catch (Exception e) {
            // Ignore me.
        }
        await().until(() -> !kafka.isRunning());
        sleep(Duration.ofSeconds(gracePeriodInSecond));

        return startKafkaBroker(port);
    }

    public static GenericContainer<?> startKafkaBroker(int port) {
        GenericContainer<?> kafka = createKafkaContainer();
        if (kafka instanceof StrimziKafkaContainer strimzi) {
            strimzi.withPort(port);
        } else if (kafka instanceof KafkaContainer k) {
            k.withPort(port);
        }
        kafka.start();
        await().until(kafka::isRunning);
        return kafka;
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stopKafkaBroker() {
        if (kafka != null) {
            try {
                kafka.stop();
            } catch (Exception e) {
                // Ignore it.
            }
            await().until(() -> !kafka.isRunning());
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.isAnnotated(KafkaBootstrapServers.class)
                && parameterContext.getParameter().getType().equals(String.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        if (parameterContext.isAnnotated(KafkaBootstrapServers.class)) {
            ExtensionContext.Store globalStore = extensionContext.getRoot().getStore(GLOBAL);
            KafkaBrokerExtension extension = (KafkaBrokerExtension) globalStore.get(KafkaBrokerExtension.class);
            return getBootstrapServers(extension.kafka);
        }
        return null;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        LOGGER.infof("Running test %s (%s#%s)", context.getDisplayName(),
                context.getTestClass().map(Class::getName).orElse(""),
                context.getTestMethod().map(Method::getName).orElse(""));
        if (kafka != null) {
            for (int i = 0; i < 3; i++) {
                try {
                    isBrokerHealthy();
                    return;
                } catch (ConditionTimeoutException e) {
                    LOGGER.warn("The Kafka broker is not healthy, restarting it");
                    restart(kafka, 0);
                }
            }
            throw new IllegalStateException("The Kafka broker is not unhealthy, despite 3 restarts");
        }
    }

    private void isBrokerHealthy() {
        await().until(() -> kafka.isRunning());
        await().catchUncaughtExceptions().until(() -> {
            Map<String, Object> config = new HashMap<>();
            config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers(kafka));
            config.put(CommonClientConfigs.CLIENT_ID_CONFIG, "broker-healthy-admin");
            try (AdminClient admin = AdminClient.create(config)) {
                Collection<Node> nodes = admin.describeCluster().nodes().get();
                return nodes.size() == 1 && nodes.iterator().next().id() >= 0;
            }
        });
    }

    public static String getBootstrapServers(GenericContainer<?> kafka) {
        if (kafka != null) {
            if (kafka instanceof StrimziKafkaContainer strimzi) {
                return strimzi.getBootstrapServers();
            } else if (kafka instanceof KafkaContainer k) {
                return k.getBootstrapServers();
            }
        }
        return null;
    }

    @Target({ ElementType.FIELD, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KafkaBootstrapServers {

    }

}
