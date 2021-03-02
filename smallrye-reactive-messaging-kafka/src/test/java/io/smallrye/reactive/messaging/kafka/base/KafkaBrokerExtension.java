package io.smallrye.reactive.messaging.kafka.base;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import io.strimzi.StrimziKafkaContainer;

public class KafkaBrokerExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    public static final Logger LOGGER = Logger.getLogger(KafkaBrokerExtension.class.getName());
    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("shopify/toxiproxy:latest");

    public static final String KAFKA_VERSION = "0.20.1-kafka-2.6.0";

    private static Network network;
    private static boolean started = false;
    private static ProxyAwareStrimziKafkaContainer kafka;
    private static ToxiproxyContainer toxiProxy;
    private static ToxiproxyContainer.ContainerProxy proxy;

    public static ToxiproxyContainer.ContainerProxy getProxy() {
        return proxy;
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        if (!started) {
            LOGGER.info("Starting Kafka broker");
            network = Network.newNetwork();
            started = true;
            startKafkaBroker();
            context.getRoot().getStore(GLOBAL).put("kafka-extension", this);
        }
    }

    @Override
    public void close() {
        LogManager.getLogManager().getLogger(KafkaBrokerExtension.class.getName()).info("Stopping Kafka broker");
        toxiProxy.stop();
        stopKafkaBroker();
        network.close();
    }

    public static String getBootstrapServers() {
        if (kafka != null) {
            return kafka.getBootstrapServers();
        }
        return null;
    }

    public static void startKafkaBroker() {
        toxiProxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withNetwork(network);
        kafka = new ProxyAwareStrimziKafkaContainer(KAFKA_VERSION)
                .withExposedPorts(9092)
                .withNetwork(network);

        toxiProxy.start();
        await()
                .until(() -> toxiProxy.isRunning());
        proxy = toxiProxy.getProxy(kafka, 9092);

        kafka.setKafkaProxy(proxy);

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

    public static class ProxyAwareStrimziKafkaContainer extends StrimziKafkaContainer {

        private ToxiproxyContainer.ContainerProxy kafkaProxy;

        public ProxyAwareStrimziKafkaContainer(String version) {
            super(version);
        }

        public void setKafkaProxy(ToxiproxyContainer.ContainerProxy kafkaProxy) {
            this.kafkaProxy = kafkaProxy;
        }

        @Override
        public ProxyAwareStrimziKafkaContainer withNetwork(Network network) {
            super.withNetwork(network);
            return this;
        }

        @Override
        public ProxyAwareStrimziKafkaContainer withExposedPorts(Integer... ports) {
            super.withExposedPorts(ports);
            return this;
        }

        @Override
        public String getBootstrapServers() {
            return String.format(
                    "PLAINTEXT://%s:%s",
                    kafkaProxy.getContainerIpAddress(),
                    kafkaProxy.getProxyPort());
        }
    }

}
