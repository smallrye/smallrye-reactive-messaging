package io.smallrye.reactive.messaging.rabbitmq;

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.util.AnnotationLiteral;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
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
                    .withLogConsumer(of -> LOGGER.info(of.getUtf8String()))
                    .waitingFor(Wait.forLogMessage(".*Server startup complete.*\\n", 1))
                    .withCopyFileToContainer(MountableFile.forClasspathResource("rabbitmq/enabled_plugins"),
                            "/etc/rabbitmq/enabled_plugins");

    protected static String host;
    protected static int port;
    protected static int managementPort;
    final static String username = "guest";
    final static String password = "guest";
    RabbitMQUsage usage;
    ExecutionHolder executionHolder;

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
        final RabbitMQConnector connector = container.getBeanManager().createInstance().select(RabbitMQConnector.class,
                new AnnotationLiteral<Any>() {
                }).get();

        // Use strict mode for health because that indicates that outgoing channels have got to the point where
        // a declared exchange has been established.
        return connector.getHealth(true).isOk();
    }

    public boolean isRabbitMQConnectorReady(SeContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getReadiness().isOk();
    }

    public boolean isRabbitMQConnectorAlive(SeContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getLiveness().isOk();
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
