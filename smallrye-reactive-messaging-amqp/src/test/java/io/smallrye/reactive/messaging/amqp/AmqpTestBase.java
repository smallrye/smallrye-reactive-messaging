package io.smallrye.reactive.messaging.amqp;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.*;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class AmqpTestBase {

    private final Logger logger = Logger.getLogger(getClass());

    ExecutionHolder executionHolder;

    @BeforeAll
    public static void setupClass(TestInfo testInfo) {
        Awaitility.setDefaultPollDelay(Duration.ZERO);
        Awaitility.setDefaultPollInterval(5, TimeUnit.MILLISECONDS);
    }

    @AfterAll
    public static void tearDownClass(TestInfo testInfo) {
        Awaitility.reset();
    }

    @BeforeEach
    public void setup(TestInfo testInfo) {
        logger.infof("========== start %s.%s ==========", getClass().getSimpleName(), testInfo.getDisplayName());

        executionHolder = new ExecutionHolder(Vertx.vertx());

        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.cleanup();
    }

    @AfterEach
    public void tearDown(TestInfo testInfo) {
        logger.infof("========== tearDown %s.%s ==========", getClass().getSimpleName(), testInfo.getDisplayName());

        executionHolder.terminate(null);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.cleanup();
    }

    public boolean isAmqpConnectorReady(AmqpConnector connector) {
        return connector.getReadiness().isOk();
    }

    public boolean isAmqpConnectorAlive(AmqpConnector connector) {
        return connector.getLiveness().isOk();
    }
}
