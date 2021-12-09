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

    private static boolean started = false;

    @Override
    public void beforeAll(ExtensionContext context) {
        if (!started) {
            LOGGER.info("Starting Kafka broker proxy");
            started = true;
            kafka = configureKafkaContainer(new ProxiedStrimziKafkaContainer());
            kafka.setNetwork(Network.newNetwork());
            kafka.start();
            await().until(() -> kafka.isRunning());
            context.getRoot().getStore(GLOBAL).put("kafka-proxy-extension", this);
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
        Object parameter = super.resolveParameter(parameterContext, extensionContext);
        if (parameter == null) {
            if (kafka != null) {
                if (parameterContext.getParameter().getType().equals(KafkaProxy.class)) {
                    return ((ProxiedStrimziKafkaContainer) kafka).getKafkaProxy();
                }
            }
        } else {
            return parameter;
        }
        return null;
    }

}
