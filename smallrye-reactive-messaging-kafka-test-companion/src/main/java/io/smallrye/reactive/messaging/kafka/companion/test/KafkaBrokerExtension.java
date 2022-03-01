package io.smallrye.reactive.messaging.kafka.companion.test;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;

import org.jboss.logging.Logger;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import io.strimzi.test.container.StrimziKafkaContainer;

/**
 * Junit extension for creating Strimzi Kafka broker
 */
public class KafkaBrokerExtension implements BeforeAllCallback, ParameterResolver, CloseableResource {
    public static final Logger LOGGER = Logger.getLogger(KafkaBrokerExtension.class.getName());

    public static final String KAFKA_VERSION = "3.1.0";

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

    @Target({ ElementType.FIELD, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KafkaBootstrapServers {

    }

}
