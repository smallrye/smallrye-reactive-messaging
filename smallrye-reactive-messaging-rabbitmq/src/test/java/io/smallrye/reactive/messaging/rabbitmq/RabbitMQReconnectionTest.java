package io.smallrye.reactive.messaging.rabbitmq;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class RabbitMQReconnectionTest extends RabbitMQBrokerTestBase {

    private WeldContainer container;

    Weld weld = new Weld();

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.select(RabbitMQConnector.class, ConnectorLiteral.of(RabbitMQConnector.CONNECTOR_NAME)).get()
                    .terminate(null);
            container.shutdown();
        }

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    private Proxy createContainerProxy(ToxiproxyContainer toxiproxy, int toxiPort) {
        try {
            // Create toxiproxy client
            ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
            // Create toxiproxy
            String upstream = "rabbitmq:5672";
            return client.createProxy(upstream, "0.0.0.0:" + toxiPort, upstream);
        } catch (IOException e) {
            throw new RuntimeException("Proxy could not be created", e);
        }
    }

    @Test
    void testSendingMessagesToRabbitMQ_connection_fails() {
        final String exchangeName = "exchg1";
        final String routingKey = "normal";

        List<Integer> received = new CopyOnWriteArrayList<>();
        usage.consumeIntegers(exchangeName, routingKey, received::add);
        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")) {
            toxiproxy.withNetwork(Network.SHARED);
            toxiproxy.start();
            await().until(toxiproxy::isRunning);

            List<Integer> exposedPorts = toxiproxy.getExposedPorts();
            int toxiPort = exposedPorts.get(exposedPorts.size() - 1);
            Proxy proxy = createContainerProxy(toxiproxy, toxiPort);
            int exposedPort = toxiproxy.getMappedPort(toxiPort);
            proxy.disable();

            weld.addBeanClass(ProducingBean.class);

            new MapBasedConfig()
                    .put("mp.messaging.outgoing.sink.exchange.name", exchangeName)
                    .put("mp.messaging.outgoing.sink.exchange.declare", false)
                    .put("mp.messaging.outgoing.sink.default-routing-key", routingKey)
                    .put("mp.messaging.outgoing.sink.connector", RabbitMQConnector.CONNECTOR_NAME)
                    .put("mp.messaging.outgoing.sink.host", toxiproxy.getHost())
                    .put("mp.messaging.outgoing.sink.port", exposedPort)
                    .put("mp.messaging.outgoing.sink.tracing.enabled", false)
                    .put("rabbitmq-username", username)
                    .put("rabbitmq-password", password)
                    .put("rabbitmq-reconnect-interval", 1)
                    .write();

            container = weld.initialize();

            await().pollDelay(3, SECONDS).until(() -> !isRabbitMQConnectorAlive(container));
            proxy.enable();
            await().until(() -> isRabbitMQConnectorAvailable(container));

            await().untilAsserted(() -> assertThat(received).hasSize(10));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testSendingMessagesToRabbitMQ_connection_fails_after_connection() {
        final String exchangeName = "exchg1";
        final String routingKey = "normal";

        List<Integer> received = new CopyOnWriteArrayList<>();
        usage.consumeIntegers(exchangeName, routingKey, received::add);
        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")) {
            toxiproxy.withNetwork(Network.SHARED);
            toxiproxy.start();
            await().until(toxiproxy::isRunning);

            List<Integer> exposedPorts = toxiproxy.getExposedPorts();
            int toxiPort = exposedPorts.get(exposedPorts.size() - 1);
            Proxy proxy = createContainerProxy(toxiproxy, toxiPort);
            int exposedPort = toxiproxy.getMappedPort(toxiPort);

            weld.addBeanClass(ProducingBean.class);

            new MapBasedConfig()
                    .put("mp.messaging.outgoing.sink.exchange.name", exchangeName)
                    .put("mp.messaging.outgoing.sink.exchange.declare", false)
                    .put("mp.messaging.outgoing.sink.default-routing-key", routingKey)
                    .put("mp.messaging.outgoing.sink.connector", RabbitMQConnector.CONNECTOR_NAME)
                    .put("mp.messaging.outgoing.sink.host", toxiproxy.getHost())
                    .put("mp.messaging.outgoing.sink.port", exposedPort)
                    .put("mp.messaging.outgoing.sink.tracing.enabled", false)
                    .put("rabbitmq-username", username)
                    .put("rabbitmq-password", password)
                    .put("rabbitmq-reconnect-interval", 1)
                    .write();

            container = weld.initialize();

            await().pollDelay(3, SECONDS).until(() -> isRabbitMQConnectorAvailable(container));
            proxy.disable();
            await().pollDelay(3, SECONDS).until(() -> !isRabbitMQConnectorAvailable(container));
            proxy.enable();

            await().untilAsserted(() -> assertThat(received).hasSize(10));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Verifies that messages can be received from RabbitMQ.
     */
    @Test
    void testReceivingMessagesFromRabbitMQ_connection_fails() {
        final String exchangeName = "exchg2";
        final String queueName = "q2";
        final String routingKey = "xyzzy";
        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")) {
            toxiproxy.withNetwork(Network.SHARED);
            toxiproxy.start();
            await().until(toxiproxy::isRunning);

            List<Integer> exposedPorts = toxiproxy.getExposedPorts();
            int toxiPort = exposedPorts.get(exposedPorts.size() - 1);
            Proxy proxy = createContainerProxy(toxiproxy, toxiPort);
            int exposedPort = toxiproxy.getMappedPort(toxiPort);

            new MapBasedConfig()
                    .put("mp.messaging.incoming.data.exchange.name", exchangeName)
                    .put("mp.messaging.incoming.data.exchange.durable", false)
                    .put("mp.messaging.incoming.data.queue.name", queueName)
                    .put("mp.messaging.incoming.data.queue.durable", true)
                    .put("mp.messaging.incoming.data.queue.routing-keys", routingKey)
                    .put("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                    .put("mp.messaging.incoming.data.host", toxiproxy.getHost())
                    .put("mp.messaging.incoming.data.port", exposedPort)
                    .put("mp.messaging.incoming.data.tracing-enabled", false)
                    .put("rabbitmq-username", username)
                    .put("rabbitmq-password", password)
                    .put("rabbitmq-reconnect-interval", 1)
                    .write();

            weld.addBeanClass(ConsumptionBean.class);

            container = weld.initialize();
            ConsumptionBean bean = get(container, ConsumptionBean.class);

            await().until(() -> isRabbitMQConnectorAvailable(container));

            List<Integer> list = bean.getResults();
            assertThat(list).isEmpty();

            AtomicInteger counter = new AtomicInteger();
            usage.produceTenIntegers(exchangeName, queueName, routingKey, counter::getAndIncrement);

            proxy.disable();
            await().pollDelay(3, SECONDS).until(() -> !isRabbitMQConnectorAvailable(container));
            proxy.enable();

            await().atMost(1, TimeUnit.MINUTES).until(() -> list.size() >= 10);
            assertThat(list).contains(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
