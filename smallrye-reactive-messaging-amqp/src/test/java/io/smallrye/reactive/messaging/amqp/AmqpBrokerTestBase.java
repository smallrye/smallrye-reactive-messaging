package io.smallrye.reactive.messaging.amqp;

import jakarta.enterprise.inject.se.SeContainer;

import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.amqp.AmqpBrokerExtension.AmqpHost;
import io.smallrye.reactive.messaging.amqp.AmqpBrokerExtension.AmqpManagementPort;
import io.smallrye.reactive.messaging.amqp.AmqpBrokerExtension.AmqpPort;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.Context;
import io.vertx.mutiny.core.Vertx;

@ExtendWith(AmqpBrokerExtension.class)
public class AmqpBrokerTestBase {

    protected ExecutionHolder executionHolder;
    protected static String host;
    protected static int port;
    protected static int managementPort;
    protected static final String username = "artemis";
    protected static final String password = "artemis";

    protected AmqpUsage usage;

    @BeforeAll
    public static void setupMutiny() {
        Infrastructure.setCanCallerThreadBeBlockedSupplier(() -> !Context.isOnEventLoopThread());
    }

    @BeforeAll
    public static void initBrokerConfig(@AmqpHost String h, @AmqpPort int p, @AmqpManagementPort int mp) {
        host = h;
        port = p;
        managementPort = mp;
        System.setProperty("amqp-host", host);
        System.setProperty("amqp-port", Integer.toString(port));
        System.setProperty("amqp-user", username);
        System.setProperty("amqp-pwd", password);
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
