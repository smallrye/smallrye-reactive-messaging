package io.smallrye.reactive.messaging.rabbitmq;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.smallrye.reactive.messaging.test.common.config.SmallRyeConfigTestUtil;
import io.vertx.core.json.JsonArray;

/**
 * Reproducer for issue #3481:
 * RabbitMQ connections unexpectedly close/reopen when multiple channels share a connection.
 * <p>
 * Tests that multiple incoming and outgoing channels sharing a single connection
 * remain stable without spurious disconnects, and that infrastructure declarations
 * (exchanges, queues, bindings) on shared connections do not interfere with each other.
 */
public class RabbitMQSharedConnectionStabilityTest extends WeldTestBase {

    private Proxy createContainerProxy(ToxiproxyContainer toxiproxy, int toxiPort) {
        try {
            ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
            String upstream = "rabbitmq:5672";
            return client.createProxy(upstream, "0.0.0.0:" + toxiPort, upstream);
        } catch (IOException e) {
            throw new RuntimeException("Proxy could not be created", e);
        }
    }

    private String uniqueName(String prefix) {
        return prefix + Math.abs(UUID.randomUUID().getMostSignificantBits());
    }

    private long countConnectionsByName(String connectionName) throws IOException {
        JsonArray connections = usage.getConnections();
        if (connections == null) {
            return 0;
        }
        return connections.stream()
                .map(o -> (JsonObject) o)
                .filter(c -> {
                    Object props = c.getValue("client_properties");
                    if (props instanceof JsonObject p) {
                        Object name = p.getValue("connection_name");
                        if (connectionName.equals(name)) {
                            return true;
                        }
                    }
                    Object userProvidedName = c.getValue("user_provided_name");
                    return connectionName.equals(userProvidedName);
                })
                .count();
    }

    /**
     * Verifies that multiple incoming channels and an outgoing channel sharing
     * a single connection work correctly: only one broker connection is created,
     * all channels receive messages, and the connection remains stable over time.
     */
    @Test
    void testMultipleChannelsSharingConnectionRemainStable() {
        String exchangeName1 = uniqueName("ex-shared1-");
        String exchangeName2 = uniqueName("ex-shared2-");
        String exchangeOut = uniqueName("ex-sharedout-");
        String queueName1 = uniqueName("q-shared1-");
        String queueName2 = uniqueName("q-shared2-");
        String routingKey = "rk";
        String sharedConnectionName = "test-shared-conn";

        weld.addBeanClass(SharedConnectionMultiChannelBean.class);
        weld.addBeanClass(OutgoingBean.class);

        MapBasedConfig config = commonConfig()
                // Incoming channel 1
                .with("mp.messaging.incoming.data1.exchange.name", exchangeName1)
                .with("mp.messaging.incoming.data1.exchange.declare", true)
                .with("mp.messaging.incoming.data1.queue.name", queueName1)
                .with("mp.messaging.incoming.data1.queue.declare", true)
                .with("mp.messaging.incoming.data1.queue.durable", true)
                .with("mp.messaging.incoming.data1.routing-keys", routingKey)
                .with("mp.messaging.incoming.data1.shared-connection-name", sharedConnectionName)
                .with("mp.messaging.incoming.data1.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data1.tracing.enabled", false)

                // Incoming channel 2 — separate exchange and queue
                .with("mp.messaging.incoming.data2.exchange.name", exchangeName2)
                .with("mp.messaging.incoming.data2.exchange.declare", true)
                .with("mp.messaging.incoming.data2.queue.name", queueName2)
                .with("mp.messaging.incoming.data2.queue.declare", true)
                .with("mp.messaging.incoming.data2.queue.durable", true)
                .with("mp.messaging.incoming.data2.routing-keys", routingKey)
                .with("mp.messaging.incoming.data2.shared-connection-name", sharedConnectionName)
                .with("mp.messaging.incoming.data2.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data2.tracing.enabled", false)

                // Outgoing channel — separate exchange, same shared connection
                .with("mp.messaging.outgoing.sink.exchange.name", exchangeOut)
                .with("mp.messaging.outgoing.sink.exchange.declare", true)
                .with("mp.messaging.outgoing.sink.default-routing-key", routingKey)
                .with("mp.messaging.outgoing.sink.shared-connection-name", sharedConnectionName)
                .with("mp.messaging.outgoing.sink.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.tracing.enabled", false);

        runApplication(config);

        SharedConnectionMultiChannelBean bean = get(SharedConnectionMultiChannelBean.class);

        assertThat(isRabbitMQConnectorAlive(container)).isTrue();
        assertThat(isRabbitMQConnectorReady(container)).isTrue();

        // Send messages directly to both queues via broker
        AtomicInteger counter1 = new AtomicInteger();
        usage.produce(exchangeName1, queueName1, routingKey, 5, counter1::getAndIncrement);

        AtomicInteger counter2 = new AtomicInteger();
        usage.produce(exchangeName2, queueName2, routingKey, 5, counter2::getAndIncrement);

        // Verify messages received on both channels
        await().atMost(30, SECONDS).untilAsserted(() -> {
            assertThat(bean.getReceivedOnData1()).hasSize(5);
            assertThat(bean.getReceivedOnData2()).hasSize(5);
        });

        // Verify only one connection to the broker with this shared connection name
        // (management API may take a moment to register connections)
        await().untilAsserted(() -> assertThat(countConnectionsByName(sharedConnectionName))
                .as("All channels should share exactly one broker connection")
                .isEqualTo(1));

        // Wait and verify the connection remains stable (no spurious disconnects)
        int data1Before = bean.getReceivedOnData1().size();
        int data2Before = bean.getReceivedOnData2().size();

        // After a delay, send another batch to confirm continued stability
        await().pollDelay(5, SECONDS)
                .untilAsserted(() -> assertThat(isRabbitMQConnectorAlive(container)).isTrue());

        counter1.set(100);
        usage.produce(exchangeName1, queueName1, routingKey, 3, counter1::getAndIncrement);
        counter2.set(100);
        usage.produce(exchangeName2, queueName2, routingKey, 3, counter2::getAndIncrement);

        await().atMost(30, SECONDS)
                .untilAsserted(() -> {
                    assertThat(bean.getReceivedOnData1().size()).isGreaterThan(data1Before);
                    assertThat(bean.getReceivedOnData2().size()).isGreaterThan(data2Before);
                });

        // Final health + connection count check
        assertThat(isRabbitMQConnectorAlive(container)).isTrue();
        assertThat(isRabbitMQConnectorReady(container)).isTrue();
        await().untilAsserted(() -> assertThat(countConnectionsByName(sharedConnectionName))
                .as("Connection should remain stable — still exactly one shared connection")
                .isEqualTo(1));
    }

    /**
     * Verifies that multiple incoming and outgoing channels each declaring
     * their own topology (exchange + queue) on a shared connection do not
     * cause connection instability during concurrent infrastructure declaration.
     */
    @Test
    void testSharedConnectionWithMultipleTopologyDeclarations() throws IOException {
        String exchangeIn1 = uniqueName("ex-topoin1-");
        String exchangeIn2 = uniqueName("ex-topoin2-");
        String exchangeOut = uniqueName("ex-topoout-");
        String queue1 = uniqueName("q-topo1-");
        String queue2 = uniqueName("q-topo2-");
        String routingKey = "rk";
        String sharedConnectionName = "test-topo-shared";

        weld.addBeanClass(SharedConnectionMultiChannelBean.class);
        weld.addBeanClass(OutgoingBean.class);

        MapBasedConfig config = commonConfig()
                // Incoming channel 1 — declares its own exchange + queue + binding
                .with("mp.messaging.incoming.data1.exchange.name", exchangeIn1)
                .with("mp.messaging.incoming.data1.exchange.declare", true)
                .with("mp.messaging.incoming.data1.exchange.durable", false)
                .with("mp.messaging.incoming.data1.queue.name", queue1)
                .with("mp.messaging.incoming.data1.queue.declare", true)
                .with("mp.messaging.incoming.data1.queue.durable", false)
                .with("mp.messaging.incoming.data1.routing-keys", routingKey)
                .with("mp.messaging.incoming.data1.shared-connection-name", sharedConnectionName)
                .with("mp.messaging.incoming.data1.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data1.tracing.enabled", false)

                // Incoming channel 2 — declares its own exchange + queue + binding
                .with("mp.messaging.incoming.data2.exchange.name", exchangeIn2)
                .with("mp.messaging.incoming.data2.exchange.declare", true)
                .with("mp.messaging.incoming.data2.exchange.durable", false)
                .with("mp.messaging.incoming.data2.queue.name", queue2)
                .with("mp.messaging.incoming.data2.queue.declare", true)
                .with("mp.messaging.incoming.data2.queue.durable", false)
                .with("mp.messaging.incoming.data2.routing-keys", routingKey)
                .with("mp.messaging.incoming.data2.shared-connection-name", sharedConnectionName)
                .with("mp.messaging.incoming.data2.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data2.tracing.enabled", false)

                // Outgoing channel — declares a third exchange on the same shared connection
                .with("mp.messaging.outgoing.sink.exchange.name", exchangeOut)
                .with("mp.messaging.outgoing.sink.exchange.declare", true)
                .with("mp.messaging.outgoing.sink.exchange.durable", false)
                .with("mp.messaging.outgoing.sink.default-routing-key", routingKey)
                .with("mp.messaging.outgoing.sink.shared-connection-name", sharedConnectionName)
                .with("mp.messaging.outgoing.sink.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.tracing.enabled", false);

        runApplication(config);

        SharedConnectionMultiChannelBean bean = get(SharedConnectionMultiChannelBean.class);

        // All topology declarations completed without disconnecting
        assertThat(isRabbitMQConnectorAlive(container)).isTrue();
        assertThat(isRabbitMQConnectorReady(container)).isTrue();

        // Verify each exchange was declared (via management API)
        assertThat(usage.getExchange(exchangeIn1)).isNotNull();
        assertThat(usage.getExchange(exchangeIn2)).isNotNull();
        assertThat(usage.getExchange(exchangeOut)).isNotNull();

        // Send messages to both incoming channels
        AtomicInteger counter = new AtomicInteger();
        usage.produce(exchangeIn1, queue1, routingKey, 5, counter::getAndIncrement);
        counter.set(0);
        usage.produce(exchangeIn2, queue2, routingKey, 5, counter::getAndIncrement);

        await().atMost(30, SECONDS)
                .untilAsserted(() -> {
                    assertThat(bean.getReceivedOnData1()).hasSize(5);
                    assertThat(bean.getReceivedOnData2()).hasSize(5);
                });

        // Only one shared connection should exist
        await().untilAsserted(() -> assertThat(countConnectionsByName(sharedConnectionName))
                .as("All channels should share exactly one broker connection")
                .isEqualTo(1));

        // Connection still healthy after all declarations and message processing
        assertThat(isRabbitMQConnectorAlive(container)).isTrue();
    }

    /**
     * Verifies that shared connection with lazy-client=false (eager connection)
     * and multiple channels declaring topology simultaneously does not cause issues.
     * This targets the specific scenario from issue #3481 where the eager client
     * creation + infrastructure declaration timing was suspected.
     */
    @Test
    void testSharedConnectionEagerClientMultipleChannels() {
        String exchange1 = uniqueName("ex-eager1-");
        String exchange2 = uniqueName("ex-eager2-");
        String exchangeOut = uniqueName("ex-eagerout-");
        String queue1 = uniqueName("q-eager1-");
        String queue2 = uniqueName("q-eager2-");
        String routingKey = "rk";
        String sharedConnectionName = "eager-shared";

        weld.addBeanClass(SharedConnectionMultiChannelBean.class);
        weld.addBeanClass(OutgoingBean.class);

        MapBasedConfig config = commonConfig()
                // Incoming channel 1 — eager client
                .with("mp.messaging.incoming.data1.exchange.name", exchange1)
                .with("mp.messaging.incoming.data1.exchange.declare", true)
                .with("mp.messaging.incoming.data1.queue.name", queue1)
                .with("mp.messaging.incoming.data1.queue.declare", true)
                .with("mp.messaging.incoming.data1.queue.durable", true)
                .with("mp.messaging.incoming.data1.routing-keys", routingKey)
                .with("mp.messaging.incoming.data1.shared-connection-name", sharedConnectionName)
                .with("mp.messaging.incoming.data1.lazy-client", false)
                .with("mp.messaging.incoming.data1.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data1.tracing.enabled", false)

                // Incoming channel 2 — eager client
                .with("mp.messaging.incoming.data2.exchange.name", exchange2)
                .with("mp.messaging.incoming.data2.exchange.declare", true)
                .with("mp.messaging.incoming.data2.queue.name", queue2)
                .with("mp.messaging.incoming.data2.queue.declare", true)
                .with("mp.messaging.incoming.data2.queue.durable", true)
                .with("mp.messaging.incoming.data2.routing-keys", routingKey)
                .with("mp.messaging.incoming.data2.shared-connection-name", sharedConnectionName)
                .with("mp.messaging.incoming.data2.lazy-client", false)
                .with("mp.messaging.incoming.data2.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data2.tracing.enabled", false)

                // Outgoing channel — eager client, separate exchange
                .with("mp.messaging.outgoing.sink.exchange.name", exchangeOut)
                .with("mp.messaging.outgoing.sink.exchange.declare", true)
                .with("mp.messaging.outgoing.sink.default-routing-key", routingKey)
                .with("mp.messaging.outgoing.sink.shared-connection-name", sharedConnectionName)
                .with("mp.messaging.outgoing.sink.lazy-client", false)
                .with("mp.messaging.outgoing.sink.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.tracing.enabled", false);

        runApplication(config);

        SharedConnectionMultiChannelBean bean = get(SharedConnectionMultiChannelBean.class);

        // Eager client should have connected and declared topology already
        assertThat(isRabbitMQConnectorAlive(container)).isTrue();

        // Produce to both channels
        AtomicInteger counter = new AtomicInteger();
        usage.produce(exchange1, queue1, routingKey, 5, counter::getAndIncrement);
        counter.set(0);
        usage.produce(exchange2, queue2, routingKey, 5, counter::getAndIncrement);

        await().atMost(30, SECONDS)
                .untilAsserted(() -> {
                    assertThat(bean.getReceivedOnData1()).hasSize(5);
                    assertThat(bean.getReceivedOnData2()).hasSize(5);
                });

        // Verify connection stable after initial eager setup + message flow
        await().pollDelay(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(isRabbitMQConnectorAlive(container)).isTrue());

        assertThat(isRabbitMQConnectorReady(container)).isTrue();
        await().untilAsserted(() -> assertThat(countConnectionsByName(sharedConnectionName))
                .as("Eager shared connection should be exactly one")
                .isEqualTo(1));
    }

    /**
     * Verifies that after a network disruption, ALL channels sharing a connection
     * recover and redeclare their topology. This is the core failure mode from
     * issue #3481: on reconnection, only some channels' topology was redeclared,
     * leaving other channels broken.
     */
    @Test
    void testMultiChannelSharedConnectionReconnectionRedeclaresAllTopology() {
        String exchange1 = uniqueName("ex-reconn1-");
        String exchange2 = uniqueName("ex-reconn2-");
        String exchangeOut = uniqueName("ex-reconnout-");
        String queue1 = uniqueName("q-reconn1-");
        String queue2 = uniqueName("q-reconn2-");
        String routingKey = "rk";
        String sharedConnectionName = "reconn-shared";

        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(
                DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
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

            weld.addBeanClass(SharedConnectionMultiChannelBean.class);
            weld.addBeanClass(OutgoingBean.class);

            new MapBasedConfig()
                    // Incoming channel 1
                    .put("mp.messaging.incoming.data1.exchange.name", exchange1)
                    .put("mp.messaging.incoming.data1.exchange.declare", true)
                    .put("mp.messaging.incoming.data1.queue.name", queue1)
                    .put("mp.messaging.incoming.data1.queue.declare", true)
                    .put("mp.messaging.incoming.data1.queue.durable", true)
                    .put("mp.messaging.incoming.data1.routing-keys", routingKey)
                    .put("mp.messaging.incoming.data1.shared-connection-name", sharedConnectionName)
                    .put("mp.messaging.incoming.data1.connector", RabbitMQConnector.CONNECTOR_NAME)
                    .put("mp.messaging.incoming.data1.host", toxiproxy.getHost())
                    .put("mp.messaging.incoming.data1.port", exposedPort)
                    .put("mp.messaging.incoming.data1.tracing.enabled", false)

                    // Incoming channel 2 — separate exchange and queue
                    .put("mp.messaging.incoming.data2.exchange.name", exchange2)
                    .put("mp.messaging.incoming.data2.exchange.declare", true)
                    .put("mp.messaging.incoming.data2.queue.name", queue2)
                    .put("mp.messaging.incoming.data2.queue.declare", true)
                    .put("mp.messaging.incoming.data2.queue.durable", true)
                    .put("mp.messaging.incoming.data2.routing-keys", routingKey)
                    .put("mp.messaging.incoming.data2.shared-connection-name", sharedConnectionName)
                    .put("mp.messaging.incoming.data2.connector", RabbitMQConnector.CONNECTOR_NAME)
                    .put("mp.messaging.incoming.data2.host", toxiproxy.getHost())
                    .put("mp.messaging.incoming.data2.port", exposedPort)
                    .put("mp.messaging.incoming.data2.tracing.enabled", false)

                    // Outgoing channel — separate exchange, same shared connection
                    .put("mp.messaging.outgoing.sink.exchange.name", exchangeOut)
                    .put("mp.messaging.outgoing.sink.exchange.declare", true)
                    .put("mp.messaging.outgoing.sink.default-routing-key", routingKey)
                    .put("mp.messaging.outgoing.sink.shared-connection-name", sharedConnectionName)
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

            SharedConnectionMultiChannelBean bean = get(container, SharedConnectionMultiChannelBean.class);

            // Verify messages flow before disconnect on BOTH channels
            AtomicInteger counter = new AtomicInteger();
            usage.produce(exchange1, queue1, routingKey, 3, counter::getAndIncrement);
            counter.set(0);
            usage.produce(exchange2, queue2, routingKey, 3, counter::getAndIncrement);

            await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                assertThat(bean.getReceivedOnData1()).isNotEmpty();
                assertThat(bean.getReceivedOnData2()).isNotEmpty();
            });

            int preDisconnectData1 = bean.getReceivedOnData1().size();
            int preDisconnectData2 = bean.getReceivedOnData2().size();

            // Disconnect
            proxy.disable();
            await().pollDelay(3, SECONDS).until(() -> !isRabbitMQConnectorAvailable(container));

            // Reconnect — all channels' topology must be re-declared
            proxy.enable();
            await().atMost(1, TimeUnit.MINUTES).until(() -> isRabbitMQConnectorAvailable(container));

            // Send messages to BOTH queues after reconnection to verify
            // each channel's topology (exchange + queue + binding) was redeclared
            counter.set(50);
            usage.produce(exchange1, queue1, routingKey, 3, counter::getAndIncrement);
            counter.set(50);
            usage.produce(exchange2, queue2, routingKey, 3, counter::getAndIncrement);

            // Both channels must receive messages after reconnection
            await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                assertThat(bean.getReceivedOnData1().size())
                        .as("Channel data1 should receive messages after reconnection")
                        .isGreaterThan(preDisconnectData1);
                assertThat(bean.getReceivedOnData2().size())
                        .as("Channel data2 should receive messages after reconnection")
                        .isGreaterThan(preDisconnectData2);
            });

            // After reconnection, all channels should still share exactly one connection
            await().untilAsserted(() -> assertThat(countConnectionsByName(sharedConnectionName))
                    .as("After reconnection, all channels should still share exactly one connection")
                    .isEqualTo(1));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @ApplicationScoped
    public static class SharedConnectionMultiChannelBean {

        private final CopyOnWriteArrayList<String> receivedOnData1 = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<String> receivedOnData2 = new CopyOnWriteArrayList<>();

        @Incoming("data1")
        public Uni<Void> consumeData1(Message<byte[]> message) {
            receivedOnData1.add(new String(message.getPayload()));
            return Uni.createFrom().completionStage(message.ack());
        }

        @Incoming("data2")
        public Uni<Void> consumeData2(Message<byte[]> message) {
            receivedOnData2.add(new String(message.getPayload()));
            return Uni.createFrom().completionStage(message.ack());
        }

        public List<String> getReceivedOnData1() {
            return receivedOnData1;
        }

        public List<String> getReceivedOnData2() {
            return receivedOnData2;
        }
    }
}
