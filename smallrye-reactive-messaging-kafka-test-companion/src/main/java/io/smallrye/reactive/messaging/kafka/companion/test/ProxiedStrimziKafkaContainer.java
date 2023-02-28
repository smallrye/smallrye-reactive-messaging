package io.smallrye.reactive.messaging.kafka.companion.test;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.List;

import org.jboss.logging.Logger;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.strimzi.test.container.StrimziKafkaContainer;

/**
 * Sets advertised listeners to the proxied port instead of exposed port
 */
public class ProxiedStrimziKafkaContainer extends StrimziKafkaContainer {

    public static final Logger LOGGER = Logger.getLogger(ProxiedStrimziKafkaContainer.class.getName());

    public static final int KAFKA_PORT = 9092;
    public static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";
    public static final String KAFKA_NETWORK_ALIAS = "kafka";
    public static final String TOXIPROXY_IMAGE_NAME_PROPERTY_KEY = "toxiproxy.image.name";
    public static final String DEFAULT_TOXIPROXY_IMAGE_NAME = "ghcr.io/shopify/toxiproxy:2.4.0";

    private final ToxiproxyContainer toxiproxy = new ToxiproxyContainer(DockerImageName.parse(
            System.getProperty(TOXIPROXY_IMAGE_NAME_PROPERTY_KEY, DEFAULT_TOXIPROXY_IMAGE_NAME))
            .asCompatibleSubstituteFor("shopify/toxiproxy"))
            .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);
    private KafkaProxy kafkaProxy;

    public ProxiedStrimziKafkaContainer() {
    }

    public ProxiedStrimziKafkaContainer(String dockerImageName) {
        super(dockerImageName);
    }

    @Override
    protected void doStart() {
        withNetworkAliases(KAFKA_NETWORK_ALIAS);
        toxiproxy
                .withNetwork(this.getNetwork())
                .start();
        await().until(toxiproxy::isRunning);
        // Pick an exposed port
        List<Integer> exposedPorts = toxiproxy.getExposedPorts();
        int toxiPort = exposedPorts.get(exposedPorts.size() - 1);
        this.kafkaProxy = createContainerProxy(toxiproxy, toxiPort);
        LOGGER.infof("Kafka proxy started : %s", this.getBootstrapServers());
        super.doStart();
    }

    private KafkaProxy createContainerProxy(ToxiproxyContainer toxiproxy, int toxiPort) {
        try {
            // Create toxiproxy client
            ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
            // Create toxiproxy
            String upstream = KAFKA_NETWORK_ALIAS + ":" + KAFKA_PORT;
            Proxy toxi = client.createProxy(upstream, "0.0.0.0:" + toxiPort, upstream);
            return new KafkaProxy(toxi, toxiproxy.getHost(), toxiproxy.getMappedPort(toxiPort), toxiPort);
        } catch (IOException e) {
            throw new RuntimeException("Proxy could not be created", e);
        }
    }

    public KafkaProxy getKafkaProxy() {
        return this.kafkaProxy;
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping Kafka proxy");
        this.toxiproxy.stop();
        super.stop();
    }

    @Override
    public String getBootstrapServers() {
        return this.kafkaProxy.getProxyBootstrapServers();
    }

}
