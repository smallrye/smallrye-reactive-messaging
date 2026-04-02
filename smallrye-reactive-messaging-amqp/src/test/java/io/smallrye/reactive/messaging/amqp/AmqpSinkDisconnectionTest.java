package io.smallrye.reactive.messaging.amqp;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.Context;
import io.vertx.mutiny.core.Vertx;

public class AmqpSinkDisconnectionTest {

    private static Network network;
    private static GenericContainer<?> artemis;
    private static final String username = "artemis";
    private static final String password = "artemis";

    private ExecutionHolder executionHolder;

    @BeforeAll
    static void startBroker() {
        Infrastructure.setCanCallerThreadBeBlockedSupplier(() -> !Context.isOnEventLoopThread());
        network = Network.newNetwork();
        String imageName = System.getProperty(AmqpBrokerExtension.ARTEMIS_IMAGE_NAME_KEY,
                AmqpBrokerExtension.ARTEMIS_IMAGE_NAME);
        artemis = new GenericContainer<>(DockerImageName.parse(imageName))
                .withExposedPorts(5672)
                .withNetworkAliases("artemis")
                .withNetwork(network)
                .withEnv("ARTEMIS_USER", username)
                .withEnv("ARTEMIS_PASSWORD", password)
                .withEnv("AMQ_ROLE", "amq")
                .withEnv("ANONYMOUS_LOGIN", "false")
                .withEnv("EXTRA_ARGS", "--http-host 0.0.0.0 --relax-jolokia")
                .waitingFor(Wait.forLogMessage(".*AMQ241004.*Artemis Console available.*\\n", 1)
                        .withStartupTimeout(Duration.ofSeconds(60)));
        artemis.start();
    }

    @AfterAll
    static void stopBroker() {
        if (artemis != null) {
            artemis.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    @BeforeEach
    void setup() {
        executionHolder = new ExecutionHolder(Vertx.vertx());
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.cleanup();
    }

    @AfterEach
    void tearDown() {
        executionHolder.terminate(null);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.cleanup();
    }

    boolean isAmqpConnectorReady(AmqpConnector connector) {
        return connector.getReadiness().isOk();
    }

    boolean isAmqpConnectorAlive(AmqpConnector connector) {
        return connector.getLiveness().isOk();
    }

    private Proxy createContainerProxy(ToxiproxyContainer toxiproxy, int toxiPort) {
        try {
            ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
            String upstream = "artemis:5672";
            return client.createProxy(upstream, "0.0.0.0:" + toxiPort, upstream);
        } catch (IOException e) {
            throw new RuntimeException("Proxy could not be created", e);
        }
    }

    @Test
    void testEagerOutgoingChannelDisconnection() {
        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(
                DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                        .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")) {
            toxiproxy.withNetwork(network);
            toxiproxy.start();
            await().until(toxiproxy::isRunning);

            List<Integer> exposedPorts = toxiproxy.getExposedPorts();
            int toxiPort = exposedPorts.get(exposedPorts.size() - 1);
            Proxy proxy = createContainerProxy(toxiproxy, toxiPort);
            int exposedPort = toxiproxy.getMappedPort(toxiPort);

            Weld weld = new Weld();
            weld.addBeanClass(MyProducer.class);

            new MapBasedConfig()
                    .with("mp.messaging.outgoing.data.connector", AmqpConnector.CONNECTOR_NAME)
                    .with("mp.messaging.outgoing.data.address", "test")
                    .with("mp.messaging.outgoing.data.host", toxiproxy.getHost())
                    .with("mp.messaging.outgoing.data.port", exposedPort)
                    .with("mp.messaging.outgoing.data.username", username)
                    .with("mp.messaging.outgoing.data.password", password)
                    .with("mp.messaging.outgoing.data.lazy-client", false)
                    .with("mp.messaging.outgoing.data.tracing-enabled", false)
                    .write();

            try (WeldContainer container = weld.initialize()) {
                AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                        ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
                await().until(() -> isAmqpConnectorReady(connector) && isAmqpConnectorAlive(connector));

                MyProducer producer = container.getBeanManager().createInstance().select(MyProducer.class).get();
                producer.produce("a");
                producer.produce("b");
                producer.produce("c");

                proxy.disable();
                await().pollDelay(2, SECONDS).until(() -> !isAmqpConnectorAlive(connector));
                proxy.enable();
                System.out.println("Proxy re-enabled");
                await().until(() -> isAmqpConnectorAlive(connector));

                System.out.println("Resending messages...");
                producer.produce("d");
                producer.produce("e");
                producer.produce("f");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testLazyOutgoingChannelDisconnection() {
        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(
                DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                        .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")) {
            toxiproxy.withNetwork(network);
            toxiproxy.start();
            await().until(toxiproxy::isRunning);

            List<Integer> exposedPorts = toxiproxy.getExposedPorts();
            int toxiPort = exposedPorts.get(exposedPorts.size() - 1);
            Proxy proxy = createContainerProxy(toxiproxy, toxiPort);
            int exposedPort = toxiproxy.getMappedPort(toxiPort);

            Weld weld = new Weld();
            weld.addBeanClass(MyProducer.class);

            new MapBasedConfig()
                    .with("mp.messaging.outgoing.data.connector", AmqpConnector.CONNECTOR_NAME)
                    .with("mp.messaging.outgoing.data.address", "test")
                    .with("mp.messaging.outgoing.data.host", toxiproxy.getHost())
                    .with("mp.messaging.outgoing.data.port", exposedPort)
                    .with("mp.messaging.outgoing.data.username", username)
                    .with("mp.messaging.outgoing.data.password", password)
                    .with("mp.messaging.outgoing.data.tracing-enabled", false)
                    .write();

            try (WeldContainer container = weld.initialize()) {
                AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                        ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
                await().until(() -> isAmqpConnectorReady(connector) && isAmqpConnectorAlive(connector));

                MyProducer producer = container.getBeanManager().createInstance().select(MyProducer.class).get();
                producer.produce("a");
                producer.produce("b");
                producer.produce("c");

                proxy.disable();
                await().pollDelay(2, SECONDS).until(() -> !isAmqpConnectorAlive(connector));
                proxy.enable();
                System.out.println("Proxy re-enabled");

                System.out.println("Resending messages...");
                producer.produce("d");
                producer.produce("e");
                producer.produce("f");
                await().until(() -> isAmqpConnectorAlive(connector));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @ApplicationScoped
    public static class MyProducer {

        @Inject
        @Channel("data")
        MutinyEmitter<String> emitter;

        public void produce(String item) {
            emitter.sendAndAwait(item);
        }
    }
}
