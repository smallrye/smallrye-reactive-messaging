package io.smallrye.reactive.messaging.amqp;

import jakarta.enterprise.inject.se.SeContainer;

import org.eclipse.microprofile.config.ConfigProvider;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class AmqpBrokerHolder {

    public static final AmqpBroker broker = new AmqpBroker();

    protected ExecutionHolder executionHolder;
    protected final static String host = "127.0.0.1";
    protected final static int port = 5672;
    protected final static String username = "artemis";
    protected final static String password = "artemis";

    protected AmqpUsage usage;

    public static void startBroker(String brokerXmlUrl) {
        if (brokerXmlUrl != null) {
            broker.setConfigResourcePath(brokerXmlUrl);
        }
        broker.start(port);
        System.setProperty("amqp-host", host);
        System.setProperty("amqp-port", Integer.toString(port));
        System.setProperty("amqp-user", username);
        System.setProperty("amqp-pwd", password);
    }

    public static void stopBroker() {
        broker.stop();
        System.clearProperty("amqp-host");
        System.clearProperty("amqp-port");
    }

    public void setup() {
        executionHolder = new ExecutionHolder(Vertx.vertx());
        usage = new AmqpUsage(executionHolder.vertx(), host, port, username, password);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.cleanup();
    }

    public void tearDown() {
        usage.close();
        executionHolder.terminate(null);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.cleanup();
    }

    public boolean isAmqpConnectorReady(SeContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getReadiness().isOk();
    }

    public boolean isAmqpConnectorReady(AmqpConnector connector) {
        return connector.getReadiness().isOk();
    }

    public boolean isAmqpConnectorAlive(SeContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getLiveness().isOk();
    }

    public boolean isAmqpConnectorAlive(AmqpConnector connector) {
        return connector.getLiveness().isOk();
    }

}
