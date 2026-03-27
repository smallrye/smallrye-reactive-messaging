package io.smallrye.reactive.messaging.rabbitmq;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;

import org.jboss.logging.Logger;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * JUnit 5 extension that manages a singleton RabbitMQ container shared across all test classes.
 * The container starts once on first use and stops when the JVM shuts down.
 */
public class RabbitMQBrokerExtension implements BeforeAllCallback, ParameterResolver, CloseableResource {

    private static final Logger LOGGER = Logger.getLogger(RabbitMQBrokerExtension.class.getName());

    public static final String RABBITMQ_IMAGE_NAME = "rabbitmq:4.2.5-management-alpine";
    public static final String RABBITMQ_IMAGE_NAME_KEY = "rabbitmq.container.image";

    private GenericContainer<?> rabbit;
    private String host;
    private int port;
    private int managementPort;

    @Override
    public void beforeAll(ExtensionContext context) {
        ExtensionContext.Store globalStore = context.getRoot().getStore(GLOBAL);
        RabbitMQBrokerExtension extension = (RabbitMQBrokerExtension) globalStore.get(RabbitMQBrokerExtension.class);
        if (extension == null) {
            LOGGER.info("Starting RabbitMQ broker");
            startBroker();
            globalStore.put(RabbitMQBrokerExtension.class, this);
        }
    }

    @Override
    public void close() {
        LOGGER.info("Stopping RabbitMQ broker");
        if (rabbit != null) {
            try {
                rabbit.stop();
            } catch (Exception e) {
                // Ignore it.
            }
        }
    }

    private void startBroker() {
        String imageName = System.getProperty(RABBITMQ_IMAGE_NAME_KEY, RABBITMQ_IMAGE_NAME);
        rabbit = new GenericContainer<>(DockerImageName.parse(imageName))
                .withExposedPorts(5672, 15672)
                .withNetworkAliases("rabbitmq")
                .withNetwork(Network.SHARED)
                .withLogConsumer(of -> LOGGER.debug(of.getUtf8String()))
                .waitingFor(Wait.forLogMessage(".*Server startup complete.*\\n", 1)
                        .withStartupTimeout(Duration.ofSeconds(30)))
                .withCopyFileToContainer(MountableFile.forClasspathResource("rabbitmq/enabled_plugins"),
                        "/etc/rabbitmq/enabled_plugins");
        rabbit.start();

        host = rabbit.getHost();
        port = rabbit.getMappedPort(5672);
        managementPort = rabbit.getMappedPort(15672);
        LOGGER.infof("RabbitMQ broker started: %s:%d (management: %d)", host, port, managementPort);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.isAnnotated(RabbitMQHost.class)
                || parameterContext.isAnnotated(RabbitMQPort.class)
                || parameterContext.isAnnotated(RabbitMQManagementPort.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        ExtensionContext.Store globalStore = extensionContext.getRoot().getStore(GLOBAL);
        RabbitMQBrokerExtension extension = (RabbitMQBrokerExtension) globalStore.get(RabbitMQBrokerExtension.class);
        if (parameterContext.isAnnotated(RabbitMQHost.class)) {
            return extension.host;
        }
        if (parameterContext.isAnnotated(RabbitMQPort.class)) {
            return extension.port;
        }
        if (parameterContext.isAnnotated(RabbitMQManagementPort.class)) {
            return extension.managementPort;
        }
        return null;
    }

    @Target({ ElementType.FIELD, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface RabbitMQHost {
    }

    @Target({ ElementType.FIELD, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface RabbitMQPort {
    }

    @Target({ ElementType.FIELD, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface RabbitMQManagementPort {
    }
}
