package io.smallrye.reactive.messaging.rabbitmq;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.smallrye.reactive.messaging.test.common.config.SmallRyeConfigTestUtil;

public class RabbitMQReconnectionTest extends WeldTestBase {

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
        final String routingKey = "normal";

        List<Integer> received = new CopyOnWriteArrayList<>();
        usage.consumeIntegers(exchangeName, routingKey, received::add);
        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")
                .withStartupAttempts(3)) {
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

            SmallRyeConfigTestUtil.installConfig();
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
        final String routingKey = "normal";

        List<Integer> received = new CopyOnWriteArrayList<>();
        usage.consumeIntegers(exchangeName, routingKey, received::add);
        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")
                .withStartupAttempts(3)) {
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
                    .put("mp.messaging.outgoing.sink.publish-confirms", true)
                    .put("rabbitmq-username", username)
                    .put("rabbitmq-password", password)
                    .put("rabbitmq-reconnect-interval", 1)
                    .write();

            SmallRyeConfigTestUtil.installConfig();
            container = weld.initialize();

            await().until(() -> isRabbitMQConnectorAvailable(container));
            proxy.disable();
            await().until(() -> !isRabbitMQConnectorAvailable(container));
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
        final String routingKey = "xyzzy";
        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")
                .withStartupAttempts(3)) {
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
                    .put("mp.messaging.incoming.data.routing-keys", routingKey)
                    .put("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                    .put("mp.messaging.incoming.data.host", toxiproxy.getHost())
                    .put("mp.messaging.incoming.data.port", exposedPort)
                    .put("mp.messaging.incoming.data.tracing.enabled", false)
                    .put("rabbitmq-username", username)
                    .put("rabbitmq-password", password)
                    .put("rabbitmq-reconnect-interval", 1)
                    .write();

            weld.addBeanClass(ConsumptionBean.class);

            SmallRyeConfigTestUtil.installConfig();
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

            await().atMost(60, SECONDS).until(() -> list.size() >= 10);
            assertThat(list).contains(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testSharedConnectionReconnectionPreservesContext() {
        final String routingKey = "shared";
        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")
                .withStartupAttempts(3)) {
            toxiproxy.withNetwork(Network.SHARED);
            toxiproxy.start();
            await().until(toxiproxy::isRunning);

            List<Integer> exposedPorts = toxiproxy.getExposedPorts();
            int toxiPort = exposedPorts.get(exposedPorts.size() - 1);
            Proxy proxy = createContainerProxy(toxiproxy, toxiPort);
            int exposedPort = toxiproxy.getMappedPort(toxiPort);

            weld.addBeanClass(ReconnectingContextBean.class);
            weld.addBeanClass(OutgoingBean.class);

            new MapBasedConfig()
                    .put("mp.messaging.incoming.data.exchange.name", exchangeName)
                    .put("mp.messaging.incoming.data.exchange.declare", true)
                    .put("mp.messaging.incoming.data.queue.name", queueName)
                    .put("mp.messaging.incoming.data.queue.declare", true)
                    .put("mp.messaging.incoming.data.queue.durable", true)
                    .put("mp.messaging.incoming.data.routing-keys", routingKey)
                    .put("mp.messaging.incoming.data.shared-connection-name", "shared-connection")
                    .put("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                    .put("mp.messaging.incoming.data.host", toxiproxy.getHost())
                    .put("mp.messaging.incoming.data.port", exposedPort)
                    .put("mp.messaging.incoming.data.tracing.enabled", false)
                    .put("mp.messaging.outgoing.sink.exchange.name", exchangeName)
                    .put("mp.messaging.outgoing.sink.exchange.declare", true)
                    .put("mp.messaging.outgoing.sink.default-routing-key", routingKey)
                    .put("mp.messaging.outgoing.sink.shared-connection-name", "shared-connection")
                    .put("mp.messaging.outgoing.sink.connector", RabbitMQConnector.CONNECTOR_NAME)
                    .put("mp.messaging.outgoing.sink.host", toxiproxy.getHost())
                    .put("mp.messaging.outgoing.sink.port", exposedPort)
                    .put("mp.messaging.outgoing.sink.tracing.enabled", false)
                    .put("rabbitmq-username", username)
                    .put("rabbitmq-password", password)
                    .put("rabbitmq-reconnect-interval", 1)
                    .write();

            SmallRyeConfigTestUtil.installConfig();
            container = weld.initialize();
            await().until(() -> isRabbitMQConnectorAvailable(container));

            ReconnectingContextBean bean = get(container, ReconnectingContextBean.class);

            AtomicInteger counter = new AtomicInteger();
            usage.produce(exchangeName, queueName, routingKey, 3, counter::getAndIncrement);

            await().atMost(1, TimeUnit.MINUTES).until(() -> !bean.getContexts().isEmpty());

            assertThat(bean.getEventLoopFlags().get(0)).isTrue();

            int preDisconnectCount = bean.getContexts().size();

            proxy.disable();
            await().pollDelay(3, SECONDS).until(() -> !isRabbitMQConnectorAvailable(container));

            proxy.enable();
            await().atMost(1, TimeUnit.MINUTES).until(() -> isRabbitMQConnectorAvailable(container));

            counter.set(0);
            usage.produce(exchangeName, queueName, routingKey, 3, counter::getAndIncrement);

            await().atMost(1, TimeUnit.MINUTES).until(() -> bean.getContexts().size() > preDisconnectCount);

            List<Boolean> postReconnectFlags = bean.getEventLoopFlags()
                    .subList(preDisconnectCount, bean.getEventLoopFlags().size());
            assertThat(postReconnectFlags)
                    .as("After reconnection, all messages should still have event loop context")
                    .doesNotContain(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Verifies that when incoming and outgoing channels share a connection,
     * ALL channels' topology is re-declared on reconnection (not just the last one registered).
     * Regression test for the single-callback overwrite bug.
     */
    @Test
    void testSharedConnectionReconnectionRedeclaresAllTopology() {
        final String routingKey = "shared-topo";
        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")
                .withStartupAttempts(3)) {
            toxiproxy.withNetwork(Network.SHARED);
            toxiproxy.start();
            await().until(toxiproxy::isRunning);

            List<Integer> exposedPorts = toxiproxy.getExposedPorts();
            int toxiPort = exposedPorts.get(exposedPorts.size() - 1);
            Proxy proxy = createContainerProxy(toxiproxy, toxiPort);
            int exposedPort = toxiproxy.getMappedPort(toxiPort);

            weld.addBeanClass(ReconnectingContextBean.class);
            weld.addBeanClass(OutgoingBean.class);

            new MapBasedConfig()
                    .put("mp.messaging.incoming.data.exchange.name", exchangeName)
                    .put("mp.messaging.incoming.data.exchange.declare", true)
                    .put("mp.messaging.incoming.data.queue.name", queueName)
                    .put("mp.messaging.incoming.data.queue.declare", true)
                    .put("mp.messaging.incoming.data.queue.durable", true)
                    .put("mp.messaging.incoming.data.routing-keys", routingKey)
                    .put("mp.messaging.incoming.data.shared-connection-name", "shared-connection")
                    .put("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                    .put("mp.messaging.incoming.data.host", toxiproxy.getHost())
                    .put("mp.messaging.incoming.data.port", exposedPort)
                    .put("mp.messaging.incoming.data.tracing.enabled", false)
                    .put("mp.messaging.outgoing.sink.exchange.name", exchangeName)
                    .put("mp.messaging.outgoing.sink.exchange.declare", true)
                    .put("mp.messaging.outgoing.sink.default-routing-key", routingKey)
                    .put("mp.messaging.outgoing.sink.shared-connection-name", "shared-connection")
                    .put("mp.messaging.outgoing.sink.connector", RabbitMQConnector.CONNECTOR_NAME)
                    .put("mp.messaging.outgoing.sink.host", toxiproxy.getHost())
                    .put("mp.messaging.outgoing.sink.port", exposedPort)
                    .put("mp.messaging.outgoing.sink.tracing.enabled", false)
                    .put("rabbitmq-username", username)
                    .put("rabbitmq-password", password)
                    .put("rabbitmq-reconnect-interval", 1)
                    .write();

            SmallRyeConfigTestUtil.installConfig();
            container = weld.initialize();
            await().until(() -> isRabbitMQConnectorAvailable(container));

            ReconnectingContextBean bean = get(container, ReconnectingContextBean.class);

            // Verify messages flow before disconnect
            AtomicInteger counter = new AtomicInteger();
            usage.produce(exchangeName, queueName, routingKey, 3, counter::getAndIncrement);
            await().atMost(1, TimeUnit.MINUTES).until(() -> !bean.getContexts().isEmpty());
            int preDisconnectCount = bean.getContexts().size();

            // Disconnect
            proxy.disable();
            await().pollDelay(3, SECONDS).until(() -> !isRabbitMQConnectorAvailable(container));

            // Reconnect — both channels' topology must be re-declared
            proxy.enable();
            await().atMost(1, TimeUnit.MINUTES).until(() -> isRabbitMQConnectorAvailable(container));

            // Send messages directly via broker to verify incoming channel's queue still works
            counter.set(0);
            usage.produce(exchangeName, queueName, routingKey, 3, counter::getAndIncrement);

            // Verify messages arrive after reconnection
            await().atMost(1, TimeUnit.MINUTES)
                    .until(() -> bean.getContexts().size() > preDisconnectCount);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Verifies that QoS (prefetch/credits) is properly re-established after reconnection.
     * This is the core concern of issue #2424: after reconnection, backpressure must be
     * respected (not reset to Long.MAX_VALUE).
     */
    @Test
    void testBackpressurePreservedAfterReconnection() {
        final String routingKey = "bp-test";
        final int prefetch = 5;

        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")
                .withStartupAttempts(3)) {
            toxiproxy.withNetwork(Network.SHARED);
            toxiproxy.start();
            await().until(toxiproxy::isRunning);

            List<Integer> exposedPorts = toxiproxy.getExposedPorts();
            int toxiPort = exposedPorts.get(exposedPorts.size() - 1);
            Proxy proxy = createContainerProxy(toxiproxy, toxiPort);
            int exposedPort = toxiproxy.getMappedPort(toxiPort);

            weld.addBeanClass(BackpressureTrackingBean.class);

            new MapBasedConfig()
                    .put("mp.messaging.incoming.data.exchange.name", exchangeName)
                    .put("mp.messaging.incoming.data.exchange.declare", true)
                    .put("mp.messaging.incoming.data.queue.name", queueName)
                    .put("mp.messaging.incoming.data.queue.declare", true)
                    .put("mp.messaging.incoming.data.queue.durable", true)
                    .put("mp.messaging.incoming.data.routing-keys", routingKey)
                    .put("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                    .put("mp.messaging.incoming.data.host", toxiproxy.getHost())
                    .put("mp.messaging.incoming.data.port", exposedPort)
                    .put("mp.messaging.incoming.data.tracing.enabled", false)
                    .put("mp.messaging.incoming.data.auto-acknowledgement", false)
                    .put("mp.messaging.incoming.data.max-outstanding-messages", prefetch)
                    .put("rabbitmq-username", username)
                    .put("rabbitmq-password", password)
                    .put("rabbitmq-reconnect-interval", 1)
                    .write();

            SmallRyeConfigTestUtil.installConfig();
            container = weld.initialize();
            await().until(() -> isRabbitMQConnectorAvailable(container));

            BackpressureTrackingBean bean = get(container, BackpressureTrackingBean.class);

            // Phase 1: verify messages flow with acking enabled
            AtomicInteger counter = new AtomicInteger();
            usage.produce(exchangeName, queueName, routingKey, 3, counter::getAndIncrement);
            await().atMost(30, SECONDS).until(() -> bean.getReceivedCount() >= 3);

            // Disconnect
            proxy.disable();
            await().pollDelay(3, SECONDS).until(() -> !isRabbitMQConnectorAvailable(container));

            // Stop acking so we can observe QoS-limited delivery after reconnection
            bean.stopAcking();
            int preReconnectCount = bean.getReceivedCount();

            // Reconnect
            proxy.enable();
            await().atMost(1, TimeUnit.MINUTES).until(() -> isRabbitMQConnectorAvailable(container));

            // Produce many more messages than the prefetch count
            counter.set(0);
            usage.produce(exchangeName, queueName, routingKey, 20, counter::getAndIncrement);

            // Wait for broker to deliver what QoS allows
            await().atMost(30, SECONDS).until(() -> bean.getReceivedCount() > preReconnectCount);
            Thread.sleep(2000);

            // After reconnection, QoS should be re-set to prefetch.
            // Without acking, the broker must not deliver more than prefetch messages.
            int postReconnectReceived = bean.getReceivedCount() - preReconnectCount;
            assertThat(postReconnectReceived)
                    .as("QoS prefetch should limit delivery to %d unacked messages after reconnection", prefetch)
                    .isLessThanOrEqualTo(prefetch);
            assertThat(postReconnectReceived)
                    .as("Some messages should arrive after reconnection")
                    .isGreaterThan(0);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ApplicationScoped
    public static class BackpressureTrackingBean {

        private final List<Message<?>> received = new CopyOnWriteArrayList<>();
        private final AtomicBoolean shouldAck = new AtomicBoolean(true);

        @Incoming("data")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public CompletionStage<Void> consume(Message<?> message) {
            received.add(message);
            if (shouldAck.get()) {
                return message.ack();
            }
            return CompletableFuture.completedFuture(null);
        }

        public List<Message<?>> getReceived() {
            return received;
        }

        public int getReceivedCount() {
            return received.size();
        }

        public void stopAcking() {
            shouldAck.set(false);
        }
    }

}
