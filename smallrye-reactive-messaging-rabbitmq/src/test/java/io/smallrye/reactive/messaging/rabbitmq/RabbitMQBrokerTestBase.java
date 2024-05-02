package io.smallrye.reactive.messaging.rabbitmq;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.UUID;

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.se.SeContainer;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

/**
 * Provides a basis for test classes, by managing the RabbitMQ broker test container.
 */
public class RabbitMQBrokerTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger("RabbitMQ");

    private static final GenericContainer<?> RABBIT = new GenericContainer<>(
            DockerImageName.parse("rabbitmq:3-management"))
            .withExposedPorts(5672, 15672)
            .withNetworkAliases("rabbitmq")
            .withNetwork(Network.SHARED)
            .withLogConsumer(of -> LOGGER.debug(of.getUtf8String()))
            .waitingFor(Wait.forLogMessage(".*Server startup complete.*\\n", 1))
            .withCopyFileToContainer(MountableFile.forClasspathResource("rabbitmq/enabled_plugins"),
                    "/etc/rabbitmq/enabled_plugins");

    protected static String host;
    protected static int port;
    protected static int managementPort;
    final static String username = "guest";
    final static String password = "guest";
    protected RabbitMQUsage usage;
    ExecutionHolder executionHolder;

    protected String exchangeName;
    protected String queueName;

    @BeforeAll
    public static void startBroker() {
        RABBIT.start();

        port = RABBIT.getMappedPort(5672);
        managementPort = RABBIT.getMappedPort(15672);
        host = RABBIT.getContainerIpAddress();

        System.setProperty("rabbitmq-host", host);
        System.setProperty("rabbitmq-port", Integer.toString(port));
        System.setProperty("rabbitmq-username", username);
        System.setProperty("rabbitmq-password", password);
    }

    @AfterAll
    public static void stopBroker() {
        RABBIT.stop();
        System.clearProperty("rabbitmq-host");
        System.clearProperty("rabbitmq-port");
    }

    @BeforeEach
    public void setup() {
        executionHolder = new ExecutionHolder(Vertx.vertx());

        usage = new RabbitMQUsage(executionHolder.vertx(), host, port, managementPort, username, password);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.cleanup();
    }

    @BeforeEach
    public void initQueueExchange(TestInfo testInfo) {
        String cn = testInfo.getTestClass().map(Class::getSimpleName).orElse(UUID.randomUUID().toString());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        queueName = "queue" + cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
        exchangeName = "exchange" + cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    @AfterEach
    public void tearDown() {
        usage.close();
        executionHolder.terminate(null);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.cleanup();
    }

    /**
     * Indicates whether the connector has completed startup (including establishment of exchanges, queues
     * and so forth)
     *
     * @param container the {@link WeldContainer}
     * @return true if the connector is ready, false otherwise
     */
    public boolean isRabbitMQConnectorAvailable(WeldContainer container) {
        final RabbitMQConnector connector = get(container, RabbitMQConnector.class, Any.Literal.INSTANCE);
        return connector.getLiveness().isOk();
    }

    public boolean isRabbitMQConnectorReady(SeContainer container) {
        HealthCenter health = get(container, HealthCenter.class);
        return health.getReadiness().isOk();
    }

    public boolean isRabbitMQConnectorAlive(SeContainer container) {
        HealthCenter health = get(container, HealthCenter.class);
        return health.getLiveness().isOk();
    }

    public <T> T get(SeContainer container, Class<T> beanType, Annotation... annotations) {
        return container.getBeanManager().createInstance().select(beanType, annotations).get();
    }

    public MapBasedConfig commonConfig() {
        return new MapBasedConfig()
                .with("rabbitmq-host", host)
                .with("rabbitmq-port", port)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("rabbitmq-reconnect-attempts", 0);
    }
}
