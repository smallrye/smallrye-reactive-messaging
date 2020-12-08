package io.smallrye.reactive.messaging.amqp;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.extension.HealthCenter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class AmqpBrokerTestBase {

    public static final AmqpBroker broker = new AmqpBroker();

    ExecutionHolder executionHolder;
    final static String host = "127.0.0.1";
    final static int port = 5672;
    final static String username = "artemis";
    final static String password = "artemis";
    AmqpUsage usage;

    @BeforeAll
    public static void startBroker() {
        broker.start();
        System.setProperty("amqp-host", host);
        System.setProperty("amqp-port", Integer.toString(port));
        System.setProperty("amqp-user", username);
        System.setProperty("amqp-pwd", password);
    }

    @AfterAll
    public static void stopBroker() {
        broker.stop();
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

    public boolean isAmqpConnectorReady(AmqpConnector connector) {
        return connector.getReadiness().isOk();
    }

    public boolean isAmqpConnectorAlive(WeldContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getLiveness().isOk();
    }

    public boolean isAmqpConnectorAlive(AmqpConnector connector) {
        return connector.getLiveness().isOk();
    }

}
