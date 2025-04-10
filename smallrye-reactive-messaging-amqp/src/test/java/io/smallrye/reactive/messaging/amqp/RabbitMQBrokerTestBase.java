package io.smallrye.reactive.messaging.amqp;

import java.time.Duration;

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
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class RabbitMQBrokerTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger("RabbitMQ");

    private static RabbitMQContainer RABBIT = new RabbitMQContainer();

    protected static String host;
    protected static int port;
    final static String username = "guest";
    final static String password = "guest";
    AmqpUsage usage;
    ExecutionHolder executionHolder;

    @BeforeAll
    public static void startBroker() {
        RABBIT.start();

        port = RABBIT.getPort();
        host = RABBIT.getHost();

        System.setProperty("amqp-host", host);
        System.setProperty("amqp-port", Integer.toString(port));
    }

    @AfterAll
    public static void stopBroker() {
        RABBIT.stop();
        System.clearProperty("amqp-host");
        System.clearProperty("amqp-port");
    }

    public static void restartBroker() {
        if (RABBIT.isRunning()) {
            stopBroker();
        }
        RABBIT = new RabbitMQContainer(port);
        startBroker();
    }

    @BeforeEach
    public void setup() {
        executionHolder = new ExecutionHolder(Vertx.vertx());

        usage = new AmqpUsage(executionHolder.vertx(), host, port, username, password);
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

    public boolean isAmqpConnectorReady(WeldContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getReadiness().isOk();
    }

    public boolean isAmqpConnectorAlive(WeldContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getLiveness().isOk();
    }

    public static class RabbitMQContainer extends GenericContainer<RabbitMQContainer> {

        public RabbitMQContainer() {
            super(DockerImageName.parse("rabbitmq:3.12-management"));
            withExposedPorts(5672, 15672);
            withReuse(true);
            //            withLogConsumer(outputFrame -> LOGGER.info(outputFrame.getUtf8String()));
            waitingFor(Wait.forLogMessage(".*Server startup complete; 4 plugins started.*\\n", 1)
                    .withStartupTimeout(Duration.ofSeconds(30)));
            withFileSystemBind("src/test/resources/rabbitmq/enabled_plugins", "/etc/rabbitmq/enabled_plugins",
                    BindMode.READ_ONLY);
        }

        public RabbitMQContainer(int port) {
            this();
            addFixedExposedPort(port, 5672);
        }

        public int getPort() {
            return getMappedPort(5672);
        }

    }

}
