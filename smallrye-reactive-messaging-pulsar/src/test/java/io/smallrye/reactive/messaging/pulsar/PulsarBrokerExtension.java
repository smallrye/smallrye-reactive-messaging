package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.PulsarContainer.PULSAR_PORT;
import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

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

/**
 * Junit extension for creating Pulsar broker
 */
public class PulsarBrokerExtension implements BeforeAllCallback, ParameterResolver, CloseableResource {
    public static final Logger LOGGER = Logger.getLogger(PulsarBrokerExtension.class.getName());

    protected PulsarContainer pulsar;

    @Override
    public void beforeAll(ExtensionContext context) {
        ExtensionContext.Store globalStore = context.getRoot().getStore(GLOBAL);
        PulsarBrokerExtension extension = (PulsarBrokerExtension) globalStore.get(PulsarBrokerExtension.class);
        if (extension == null) {
            LOGGER.info("Starting Pulsar broker");
            startPulsarBroker();
            globalStore.put(PulsarBrokerExtension.class, this);
        }
    }

    @Override
    public void close() {
        LOGGER.info("Stopping Pulsar broker");
        stopPulsarBroker();
    }

    public static PulsarContainer createPulsarContainer() {
        return new PulsarContainer();
    }

    public void startPulsarBroker() {
        pulsar = createPulsarContainer();
        pulsar.start();
        LOGGER.info("Pulsar broker started: " + pulsar.getClusterServiceUrl() + " (" + pulsar.getMappedPort(PULSAR_PORT) + ")");
        await().until(() -> pulsar.isRunning());
    }

    /**
     * We need to restart the broker on the same exposed port.
     * Test Containers makes this unnecessarily complicated, but well, let's go for a hack.
     * See https://github.com/testcontainers/testcontainers-java/issues/256.
     *
     * @param pulsar the broker that will be closed
     * @param gracePeriodInSecond number of seconds to wait before restarting
     * @return the new broker
     */
    public static PulsarContainer restart(PulsarContainer pulsar, int gracePeriodInSecond) {
        int port = pulsar.getMappedPort(PULSAR_PORT);
        try {
            pulsar.close();
        } catch (Exception e) {
            // Ignore me.
        }
        await().until(() -> !pulsar.isRunning());
        sleep(Duration.ofSeconds(gracePeriodInSecond));

        return startPulsarBroker(port);
    }

    public static PulsarContainer startPulsarBroker(int port) {
        PulsarContainer Pulsar = createPulsarContainer();
        Pulsar.start();
        await().until(Pulsar::isRunning);
        return Pulsar;
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stopPulsarBroker() {
        if (pulsar != null) {
            try {
                pulsar.stop();
            } catch (Exception e) {
                // Ignore it.
            }
            await().until(() -> !pulsar.isRunning());
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.isAnnotated(PulsarServiceUrl.class)
                && parameterContext.getParameter().getType().equals(String.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        if (parameterContext.isAnnotated(PulsarServiceUrl.class)) {
            ExtensionContext.Store globalStore = extensionContext.getRoot().getStore(GLOBAL);
            PulsarBrokerExtension extension = (PulsarBrokerExtension) globalStore.get(PulsarBrokerExtension.class);
            if (extension.pulsar != null) {
                return extension.pulsar.getClusterServiceUrl();
            }
        }
        return null;
    }

    @Target({ ElementType.FIELD, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface PulsarServiceUrl {

    }

}
