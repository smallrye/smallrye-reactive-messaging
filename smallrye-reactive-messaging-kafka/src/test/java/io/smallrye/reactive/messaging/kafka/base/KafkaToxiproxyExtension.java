package io.smallrye.reactive.messaging.kafka.base;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

import java.io.IOException;
import java.util.List;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;

public class KafkaToxiproxyExtension extends KafkaBrokerExtension
        implements BeforeAllCallback, ParameterResolver, CloseableResource {
    public static final Logger LOGGER = Logger.getLogger(KafkaToxiproxyExtension.class.getName());

    public static final int KAFKA_PORT = 9092;
    public static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";
    public static final String KAFKA_NETWORK_ALIAS = "kafka";
    public static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("shopify/toxiproxy:2.1.0");

    private static boolean started = false;
    private static ToxiproxyContainer toxiproxy;
    private static ContainerProxy kafkaproxy;

    @Override
    public void beforeAll(ExtensionContext context) {
        if (!started) {
            LOGGER.info("Starting Kafka broker proxy");
            started = true;
            Network network = Network.newNetwork();
            int toxiPort = startKafkaProxy(network);
            startKafkaBroker(network, toxiproxy.getMappedPort(toxiPort));
            context.getRoot().getStore(GLOBAL).put("kafka-proxy-extension", this);
        }
    }

    public static String getProxyBootstrapServers(ContainerProxy proxy) {
        return String.format("PLAINTEXT://%s:%s", proxy.containerIpAddress, proxy.proxyPort);
    }

    static int startKafkaProxy(Network network) {
        toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);
        toxiproxy.start();
        await().until(() -> toxiproxy.isRunning());
        // Pick an exposed port
        List<Integer> exposedPorts = toxiproxy.getExposedPorts();
        int toxiPort = exposedPorts.get(exposedPorts.size() - 1);
        kafkaproxy = createContainerProxy(toxiPort);
        LOGGER.info(
                "Kafka proxy started: " + getProxyBootstrapServers(kafkaproxy) + " (" + kafkaproxy.proxyPort + ")");
        return toxiPort;
    }

    static void startKafkaBroker(Network network, int proxyPort) {
        kafka = KafkaBrokerExtension.createKafkaContainer()
                .withNetwork(network)
                .withNetworkAliases(KAFKA_NETWORK_ALIAS)
                .withBootstrapServers(c -> String.format("PLAINTEXT://%s:%s", c.getContainerIpAddress(), proxyPort));
        kafka.start();
        LOGGER.info("Kafka broker started: (" + kafka.getMappedPort(9092) + ")");
        await().until(() -> kafka.isRunning());
    }

    static ContainerProxy createContainerProxy(int toxiPort) {
        try {
            // Create toxiproxy client
            ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
            // Create toxiproxy
            String upstream = KAFKA_NETWORK_ALIAS + ":" + KAFKA_PORT;
            Proxy toxi = client.createProxy(upstream, "0.0.0.0:" + toxiPort, upstream);
            return new ContainerProxy(toxi, toxiproxy.getHost(), toxiproxy.getMappedPort(toxiPort), toxiPort);
        } catch (IOException e) {
            throw new RuntimeException("Proxy could not be created", e);
        }
    }

    @Override
    public void close() {
        super.close();
        LogManager.getLogManager().getLogger(KafkaToxiproxyExtension.class.getName()).info("Stopping Kafka proxy");
        stopKafkaProxy();
    }

    public static void stopKafkaProxy() {
        if (toxiproxy != null) {
            try {
                toxiproxy.stop();
            } catch (Exception e) {
                // Ignore it.
            }
            await().until(() -> !toxiproxy.isRunning());
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return super.supportsParameter(parameterContext, extensionContext)
                || parameterContext.getParameter().getType().equals(ContainerProxy.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        if (kafkaproxy != null) {
            if (parameterContext.getParameter().getType().equals(ContainerProxy.class)) {
                return kafkaproxy;
            }
            if (parameterContext.isAnnotated(KafkaBootstrapServers.class)) {
                return getProxyBootstrapServers(kafkaproxy);
            }
        }
        return null;
    }

    /**
     * Test containers Toxiproxy doesn't give access to underlying Proxy object
     */
    public static class ContainerProxy {
        public final Proxy toxi;
        public final String containerIpAddress;
        public final int proxyPort;
        public final int originalProxyPort;

        protected ContainerProxy(Proxy toxi, String containerIpAddress, int proxyPort, int originalProxyPort) {
            this.toxi = toxi;
            this.containerIpAddress = containerIpAddress;
            this.proxyPort = proxyPort;
            this.originalProxyPort = originalProxyPort;
        }

    }

}
