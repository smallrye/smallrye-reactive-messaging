package io.smallrye.reactive.messaging.rabbitmq;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.se.SeContainer;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

/**
 * Tests RabbitMQ reconnection scenarios using Toxiproxy for fault injection.
 * Uses its own dedicated RabbitMQ broker to avoid disrupting the shared container.
 */
public class RabbitMQReconnectionTest {

    private static Network network;
    private static GenericContainer<?> rabbit;
    private static String host;
    private static int port;
    private static int managementPort;
    static final String username = "guest";
    static final String password = "guest";

    private RabbitMQUsage usage;
    private ExecutionHolder executionHolder;

    private String exchangeName;
    private String queueName;

    private WeldContainer container;
    Weld weld = new Weld();

    @BeforeAll
    static void startBroker() {
        network = Network.newNetwork();
        rabbit = new GenericContainer<>(DockerImageName.parse("rabbitmq:3.12-management"))
                .withExposedPorts(5672, 15672)
                .withNetworkAliases("rabbitmq")
                .withNetwork(network)
                .waitingFor(Wait.forLogMessage(".*Server startup complete.*\\n", 1)
                        .withStartupTimeout(Duration.ofSeconds(30)))
                .withCopyFileToContainer(MountableFile.forClasspathResource("rabbitmq/enabled_plugins"),
                        "/etc/rabbitmq/enabled_plugins");
        rabbit.start();

        host = rabbit.getHost();
        port = rabbit.getMappedPort(5672);
        managementPort = rabbit.getMappedPort(15672);

        System.setProperty("rabbitmq-host", host);
        System.setProperty("rabbitmq-port", Integer.toString(port));
        System.setProperty("rabbitmq-username", username);
        System.setProperty("rabbitmq-password", password);
    }

    @AfterAll
    static void stopBroker() {
        if (rabbit != null) {
            rabbit.stop();
        }
        if (network != null) {
            network.close();
        }
        System.clearProperty("rabbitmq-host");
        System.clearProperty("rabbitmq-port");
    }

    @BeforeEach
    void setup() {
        executionHolder = new ExecutionHolder(Vertx.vertx());
        usage = new RabbitMQUsage(executionHolder.vertx(), host, port, managementPort, username, password);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.cleanup();
    }

    @BeforeEach
    void initQueueExchange(TestInfo testInfo) {
        String cn = testInfo.getTestClass().map(Class::getSimpleName).orElse(UUID.randomUUID().toString());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        queueName = "queue" + cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
        exchangeName = "exchange" + cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    @AfterEach
    void tearDown() {
        usage.close();
        executionHolder.terminate(null);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.cleanup();
    }

    @AfterEach
    void cleanup() {
        if (container != null) {
            container.select(RabbitMQConnector.class, ConnectorLiteral.of(RabbitMQConnector.CONNECTOR_NAME)).get()
                    .terminate(null);
            container.shutdown();
        }

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    boolean isRabbitMQConnectorAvailable(WeldContainer container) {
        final RabbitMQConnector connector = get(container, RabbitMQConnector.class, Any.Literal.INSTANCE);
        return connector.getLiveness().isOk();
    }

    boolean isRabbitMQConnectorAlive(SeContainer container) {
        HealthCenter health = get(container, HealthCenter.class);
        return health.getLiveness().isOk();
    }

    <T> T get(SeContainer container, Class<T> beanType, Annotation... annotations) {
        return container.getBeanManager().createInstance().select(beanType, annotations).get();
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

    @Test // 15s
    void testSendingMessagesToRabbitMQ_connection_fails() {
        final String routingKey = "normal";

        List<Integer> received = new CopyOnWriteArrayList<>();
        usage.consumeIntegers(exchangeName, routingKey, received::add);
        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")) {
            toxiproxy.withNetwork(network);
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

    @Test // 17s
    void testSendingMessagesToRabbitMQ_connection_fails_after_connection() {
        final String routingKey = "normal";

        List<Integer> received = new CopyOnWriteArrayList<>();
        usage.consumeIntegers(exchangeName, routingKey, received::add);
        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")) {
            toxiproxy.withNetwork(network);
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

    @Test
    void testSharedConnectionReconnectionPreservesContext() {
        final String routingKey = "shared";
        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")) {
            toxiproxy.withNetwork(network);
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

            container = weld.initialize();
            await().until(() -> isRabbitMQConnectorAvailable(container));

            ReconnectingContextBean bean = get(container, ReconnectingContextBean.class);

            // Wait for at least one message before disconnect (from OutgoingBean)
            await().atMost(1, TimeUnit.MINUTES).until(() -> !bean.getContexts().isEmpty());

            // Verify pre-disconnect messages have event loop context
            assertThat(bean.getEventLoopFlags().get(0)).isTrue();

            int preDisconnectCount = bean.getContexts().size();

            // Disconnect
            proxy.disable();
            await().pollDelay(3, SECONDS).until(() -> !isRabbitMQConnectorAvailable(container));

            // Reconnect
            proxy.enable();
            await().atMost(1, TimeUnit.MINUTES).until(() -> isRabbitMQConnectorAvailable(container));

            // Send messages after reconnection via the direct RabbitMQ client
            AtomicInteger counter = new AtomicInteger();
            usage.produce(exchangeName, queueName, routingKey, 3, counter::getAndIncrement);

            // Wait for at least one more message after reconnection
            await().atMost(1, TimeUnit.MINUTES).until(() -> bean.getContexts().size() > preDisconnectCount);

            // Verify post-reconnection messages also have event loop context.
            // This should fail because the reconnection Uni closure captures the original
            // null root parameter instead of reading rootContext.get(), so after reconnect
            // the context falls back to Vertx.currentContext() which may not be an event loop.
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
     * Verifies that messages can be received from RabbitMQ.
     */
    @Test // 14s
    void testReceivingMessagesFromRabbitMQ_connection_fails() {
        final String routingKey = "xyzzy";
        try (ToxiproxyContainer toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest")
                .asCompatibleSubstituteFor("shopify/toxiproxy"))
                .withNetworkAliases("toxiproxy")) {
            toxiproxy.withNetwork(network);
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
