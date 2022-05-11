package io.smallrye.reactive.messaging.amqp.ssl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.amqp.AmqpBrokerTestBase;
import io.smallrye.reactive.messaging.amqp.AmqpConnector;
import io.smallrye.reactive.messaging.amqp.AmqpUsage;
import io.smallrye.reactive.messaging.amqp.ConsumptionBean;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class AmqpSSLSourceCDISSLContextTest extends AmqpBrokerTestBase {

    private AmqpConnector provider;

    private WeldContainer container;

    @BeforeAll
    public static void startBroker() {
        try {
            String brokerXml = SSLBrokerConfigUtil.createSecuredBrokerXml();
            System.setProperty(BROKER_XML_LOCATION, brokerXml);
            AmqpBrokerTestBase.startBroker();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void clearSslBrokerName() {
        System.clearProperty(BROKER_XML_LOCATION);
    }

    @Override
    @BeforeEach
    public void setup() {
        super.setup();
        // Override the usage port
        usage.close();
        usage = new AmqpUsage(executionHolder.vertx(), host, port + 1, username, password);
    }

    @AfterEach
    public void cleanup() {
        if (provider != null) {
            provider.terminate(null);
        }

        if (container != null) {
            container.shutdown();
        }

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());

        System.clearProperty("mp-config");
        System.clearProperty("client-options-name");
        System.clearProperty("amqp-client-options-name");
    }

    @Test
    public void testSuppliedSslContextGlobal() {
        Weld weld = new Weld();

        String address = UUID.randomUUID().toString();
        weld.addBeanClass(ClientSslContextBean.class);
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.address", address)
                .with("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .with("amqp-use-ssl", "true")
                .with("amqp-client-ssl-context-name", "mysslcontext")
                .write();

        container = weld.initialize();
        await().until(() -> isAmqpConnectorAlive(container));
        await().until(() -> isAmqpConnectorReady(container));
        List<Integer> list = container.select(ConsumptionBean.class).get().getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers(address, counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testSuppliedSslContextConnector() {
        Weld weld = new Weld();

        String address = UUID.randomUUID().toString();
        weld.addBeanClass(ClientSslContextBean.class);
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.address", address)
                .with("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("mp.messaging.incoming.data.username", username)
                .with("mp.messaging.incoming.data.password", password)
                .with("mp.messaging.incoming.data.use-ssl", "true")
                .with("mp.messaging.incoming.data.client-ssl-context-name", "mysslcontext")
                .write();

        container = weld.initialize();
        await().until(() -> isAmqpConnectorAlive(container));
        await().until(() -> isAmqpConnectorReady(container));
        List<Integer> list = container.select(ConsumptionBean.class).get().getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers(address, counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }
}
