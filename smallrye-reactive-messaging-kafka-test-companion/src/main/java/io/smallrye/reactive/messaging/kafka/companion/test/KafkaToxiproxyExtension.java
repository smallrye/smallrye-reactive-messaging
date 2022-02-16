package io.smallrye.reactive.messaging.kafka.companion.test;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.util.logging.Logger;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.testcontainers.containers.Network;

/**
 * Junit extension for creating Strimzi Kafka broker behind a Toxiproxy
 */
public class KafkaToxiproxyExtension extends KafkaBrokerExtension
        implements BeforeAllCallback, ParameterResolver, CloseableResource {
    public static final Logger LOGGER = Logger.getLogger(KafkaToxiproxyExtension.class.getName());

    @Override
    public void beforeAll(ExtensionContext context) {
        ExtensionContext.Store globalStore = context.getRoot().getStore(GLOBAL);
        KafkaToxiproxyExtension extension = (KafkaToxiproxyExtension) globalStore.get(KafkaToxiproxyExtension.class);
        if (extension == null) {
            LOGGER.info("Starting Kafka broker proxy");
            kafka = configureKafkaContainer(new ProxiedStrimziKafkaContainer());
            kafka.setNetwork(Network.newNetwork());
            kafka.start();
            await().until(() -> kafka.isRunning());
            globalStore.put(KafkaToxiproxyExtension.class, this);
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return super.supportsParameter(parameterContext, extensionContext)
                || parameterContext.getParameter().getType().equals(KafkaProxy.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        ExtensionContext.Store globalStore = extensionContext.getRoot().getStore(GLOBAL);
        KafkaToxiproxyExtension extension = (KafkaToxiproxyExtension) globalStore.get(KafkaToxiproxyExtension.class);
        if (extension != null) {
            if (extension.kafka != null) {
                if (parameterContext.isAnnotated(KafkaBootstrapServers.class)) {
                    return extension.kafka.getBootstrapServers();
                }
                if (parameterContext.getParameter().getType().equals(KafkaProxy.class)) {
                    return ((ProxiedStrimziKafkaContainer) extension.kafka).getKafkaProxy();
                }
            }
        }
        return null;
    }

}
