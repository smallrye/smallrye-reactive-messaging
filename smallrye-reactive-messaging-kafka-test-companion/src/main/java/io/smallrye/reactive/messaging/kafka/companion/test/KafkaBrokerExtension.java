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
import org.testcontainers.shaded.org.awaitility.core.ConditionTimeoutException;

import io.strimzi.test.container.StrimziKafkaContainer;

/**
 * Junit extension for creating Strimzi Kafka broker
 */
public class KafkaBrokerExtension implements BeforeAllCallback, BeforeEachCallback, ParameterResolver, CloseableResource {
    public static final Logger LOGGER = Logger.getLogger(KafkaBrokerExtension.class.getName());

    public static final String KAFKA_VERSION = "3.3.2";

    protected StrimziKafkaContainer kafka;

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

    public static StrimziKafkaContainer createKafkaContainer() {
        return configureKafkaContainer(new StrimziKafkaContainer());
    }

    public static <T extends StrimziKafkaContainer> T configureKafkaContainer(T container) {
        String kafkaVersion = System.getProperty("kafka-container-version", KAFKA_VERSION);
        container.withKafkaVersion(kafkaVersion);
        Map<String, String> config = new HashMap<>();
        config.put("log.cleaner.enable", "false");
        container.withKafkaConfigurationMap(config);
        return container;
    }

    public void startKafkaBroker() {
        kafka = createKafkaContainer();
        kafka.start();
        LOGGER.info("Kafka broker started: " + kafka.getBootstrapServers() + " (" + kafka.getMappedPort(9092) + ")");
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
    public static StrimziKafkaContainer restart(StrimziKafkaContainer kafka, int gracePeriodInSecond) {
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

    public static StrimziKafkaContainer startKafkaBroker(int port) {
        StrimziKafkaContainer kafka = createKafkaContainer().withPort(port);
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
            if (extension.kafka != null) {
                return extension.kafka.getBootstrapServers();
            }
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
            config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            config.put(CommonClientConfigs.CLIENT_ID_CONFIG, "broker-healthy-admin");
            try (AdminClient admin = AdminClient.create(config)) {
                Collection<Node> nodes = admin.describeCluster().nodes().get();
                return nodes.size() == 1 && nodes.iterator().next().id() >= 0;
            }
        });
    }

    @Target({ ElementType.FIELD, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KafkaBootstrapServers {

    }

}
