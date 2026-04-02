package io.smallrye.reactive.messaging.amqp;

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

/**
 * JUnit 5 extension that manages a singleton Artemis container shared across all test classes.
 * The container starts once on first use and stops when the JVM shuts down.
 */
public class AmqpBrokerExtension implements BeforeAllCallback, ParameterResolver, CloseableResource {

    private static final Logger LOGGER = Logger.getLogger(AmqpBrokerExtension.class.getName());

    public static final String ARTEMIS_IMAGE_NAME = "apache/artemis:2.53.0";
    public static final String ARTEMIS_IMAGE_NAME_KEY = "artemis.container.image";

    private GenericContainer<?> artemis;
    private String host;
    private int port;
    private int managementPort;

    @Override
    public void beforeAll(ExtensionContext context) {
        ExtensionContext.Store globalStore = context.getRoot().getStore(GLOBAL);
        AmqpBrokerExtension extension = (AmqpBrokerExtension) globalStore.get(AmqpBrokerExtension.class);
        if (extension == null) {
            LOGGER.info("Starting Artemis broker");
            startBroker();
            globalStore.put(AmqpBrokerExtension.class, this);
        }
    }

    @Override
    public void close() {
        LOGGER.info("Stopping Artemis broker");
        if (artemis != null) {
            try {
                artemis.stop();
            } catch (Exception e) {
                // Ignore it.
            }
        }
    }

    private void startBroker() {
        String imageName = System.getProperty(ARTEMIS_IMAGE_NAME_KEY, ARTEMIS_IMAGE_NAME);
        artemis = new GenericContainer<>(DockerImageName.parse(imageName))
                .withExposedPorts(5672, 8161)
                .withNetworkAliases("artemis")
                .withNetwork(Network.SHARED)
                .withEnv("ARTEMIS_USER", "artemis")
                .withEnv("ARTEMIS_PASSWORD", "artemis")
                .withEnv("AMQ_ROLE", "amq")
                .withEnv("ANONYMOUS_LOGIN", "false")
                .withEnv("EXTRA_ARGS", "--http-host 0.0.0.0 --relax-jolokia")
                .withLogConsumer(of -> LOGGER.debug(of.getUtf8String()))
                .waitingFor(Wait.forLogMessage(".*AMQ241004.*Artemis Console available.*\\n", 1)
                        .withStartupTimeout(Duration.ofSeconds(60)));
        artemis.start();

        host = artemis.getHost();
        port = artemis.getMappedPort(5672);
        managementPort = artemis.getMappedPort(8161);
        LOGGER.infof("Artemis broker started: %s:%d (management: %d)", host, port, managementPort);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.isAnnotated(AmqpHost.class)
                || parameterContext.isAnnotated(AmqpPort.class)
                || parameterContext.isAnnotated(AmqpManagementPort.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        ExtensionContext.Store globalStore = extensionContext.getRoot().getStore(GLOBAL);
        AmqpBrokerExtension extension = (AmqpBrokerExtension) globalStore.get(AmqpBrokerExtension.class);
        if (parameterContext.isAnnotated(AmqpHost.class)) {
            return extension.host;
        }
        if (parameterContext.isAnnotated(AmqpPort.class)) {
            return extension.port;
        }
        if (parameterContext.isAnnotated(AmqpManagementPort.class)) {
            return extension.managementPort;
        }
        return null;
    }

    @Target({ ElementType.FIELD, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface AmqpHost {
    }

    @Target({ ElementType.FIELD, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface AmqpPort {
    }

    @Target({ ElementType.FIELD, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface AmqpManagementPort {
    }
}
