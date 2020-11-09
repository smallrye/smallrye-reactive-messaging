package io.smallrye.reactive.messaging.kafka.base;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.strimzi.StrimziKafkaContainer;

public class KafkaBrokerExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    public static final Logger LOGGER = Logger.getLogger(KafkaBrokerExtension.class.getName());
    private static boolean started = false;
    private static StrimziKafkaContainer kafka;

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

    public static String getBootstrapServers() {
        if (kafka != null) {
            return kafka.getBootstrapServers();
        }
        return null;
    }

    public static void startKafkaBroker() {
        kafka = new StrimziKafkaContainer();
        kafka.start();
        LOGGER.info("Kafka broker started: " + kafka.getBootstrapServers());
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

}
