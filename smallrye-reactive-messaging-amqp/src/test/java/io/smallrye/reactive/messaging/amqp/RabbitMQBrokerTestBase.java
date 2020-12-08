package io.smallrye.reactive.messaging.amqp;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.extension.HealthCenter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class RabbitMQBrokerTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger("RabbitMQ");

    private static final GenericContainer<?> RABBIT = new GenericContainer<>(DockerImageName.parse("rabbitmq:3-management"))
            .withExposedPorts(5672, 15672)
            .withLogConsumer(of -> LOGGER.info(of.getUtf8String()))
            .waitingFor(
                    Wait.forLogMessage(".*Server startup complete; 4 plugins started.*\\n", 1))
            .withFileSystemBind("src/test/resources/rabbitmq/enabled_plugins", "/etc/rabbitmq/enabled_plugins",
                    BindMode.READ_ONLY);

    protected static String host;
    protected static int port;
    final static String username = "guest";
    final static String password = "guest";
    AmqpUsage usage;
    ExecutionHolder executionHolder;

    @BeforeAll
    public static void startBroker() {
        RABBIT.start();

        port = RABBIT.getMappedPort(5672);
        host = RABBIT.getContainerIpAddress();

        System.setProperty("amqp-host", host);
        System.setProperty("amqp-port", Integer.toString(port));
    }

    @AfterAll
    public static void stopBroker() {
        RABBIT.stop();
        System.clearProperty("amqp-host");
        System.clearProperty("amqp-port");
    }

    @BeforeEach
    public void setup() {
        executionHolder = new ExecutionHolder(Vertx.vertx());

        usage = new AmqpUsage(executionHolder.vertx(), host, port, username, password);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.clear();
    }

    @AfterEach
    public void tearDown() {
        usage.close();
        executionHolder.terminate(null);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.clear();
    }

    public boolean isAmqpConnectorReady(WeldContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getReadiness().isOk();
    }

    public boolean isAmqpConnectorAlive(WeldContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getLiveness().isOk();
    }

}
