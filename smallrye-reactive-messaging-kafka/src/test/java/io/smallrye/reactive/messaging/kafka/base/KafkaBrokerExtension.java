package io.smallrye.reactive.messaging.kafka.base;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import io.strimzi.StrimziKafkaContainer;

public class KafkaBrokerExtension implements BeforeAllCallback, ParameterResolver, CloseableResource {
    public static final Logger LOGGER = Logger.getLogger(KafkaBrokerExtension.class.getName());

    public static final String KAFKA_VERSION = "latest-kafka-3.0.0";

    private static boolean started = false;
    static StrimziKafkaContainer kafka;

    @Override
    public void beforeAll(ExtensionContext context) {
        if (!started) {
            LOGGER.info("Starting Kafka broker");
            started = true;
            startKafkaBroker();
            context.getRoot().getStore(GLOBAL).put("kafka-extension", this);
        }
    }

    @Override
    public void close() {
        LogManager.getLogManager().getLogger(KafkaBrokerExtension.class.getName()).info("Stopping Kafka broker");
        stopKafkaBroker();
    }

    public static String getKafkaContainerVersion() {
        String kafkaContainerVersion = System.getProperty("kafka-container-version");
        return kafkaContainerVersion != null ? kafkaContainerVersion : KAFKA_VERSION;
    }

    public static void startKafkaBroker() {
        kafka = new StrimziKafkaContainer(getKafkaContainerVersion())
                .withExposedPorts(9092);
        kafka.start();
        LOGGER.info("Kafka broker started: " + kafka.getBootstrapServers() + " (" + kafka.getMappedPort(9092) + ")");
        await().until(() -> kafka.isRunning());

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
            if (kafka != null) {
                return kafka.getBootstrapServers();
            }
        }
        return null;
    }

    @Target({ ElementType.FIELD, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KafkaBootstrapServers {

    }

}
