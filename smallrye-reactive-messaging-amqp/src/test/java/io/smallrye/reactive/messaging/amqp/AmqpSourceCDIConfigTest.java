package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.CHANNEL_NAME_ATTRIBUTE;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.jboss.weld.exceptions.DeploymentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.common.constraint.NotNull;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class AmqpSourceCDIConfigTest extends AmqpBrokerTestBase {

    private AmqpConnector provider;

    private WeldContainer container;

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
    public void testConfigByCDIMissingBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(ExecutionHolder.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.address", "data")
                .with("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .with("mp.messaging.incoming.data.client-options-name", "myclientoptions")
                .write();

        assertThatThrownBy(() -> container = weld.initialize())
                .isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigByCDIIncorrectBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);
        weld.addBeanClass(ExecutionHolder.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.address", "data")
                .with("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .with("mp.messaging.incoming.data.client-options-name", "dummyoptionsnonexistent")
                .write();

        assertThatThrownBy(() -> container = weld.initialize())
                .isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigByCDICorrect() {
        Weld weld = new Weld();

        weld.addBeanClass(ClientConfigurationBean.class);
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.address", "data")
                .with("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .with("mp.messaging.incoming.data.client-options-name", "myclientoptions")
                .write();

        container = weld.initialize();
        await().until(() -> isAmqpConnectorAlive(container));
        await().until(() -> isAmqpConnectorReady(container));
        List<Integer> list = container.select(ConsumptionBean.class).get().getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers("data", counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testConfigGlobalOptionsByCDICorrect() {
        Weld weld = new Weld();

        String address = UUID.randomUUID().toString();
        weld.addBeanClass(ClientConfigurationBean.class);
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.address", address)
                .with("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .with("amqp-client-options-name", "myclientoptions")
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
    public void testConfigGlobalOptionsByCDIMissingBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(ExecutionHolder.class);

        new MapBasedConfig()
                .put("mp.messaging.incoming.data.address", "data")
                .put("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("amqp-client-options-name", "myclientoptions")
                .write();

        assertThatThrownBy(() -> container = weld.initialize())
                .isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigGlobalOptionsByCDIIncorrectBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);
        weld.addBeanClass(ExecutionHolder.class);

        new MapBasedConfig()
                .put("mp.messaging.incoming.data.address", "data")
                .put("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("amqp-client-options-name", "dummyoptionsnonexistent")
                .write();

        assertThatThrownBy(() -> container = weld.initialize())
                .isInstanceOf(DeploymentException.class);
    }

    @NotNull
    private Map<String, Object> getConfig(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put("address", topic);
        config.put(CHANNEL_NAME_ATTRIBUTE, UUID.randomUUID().toString());
        config.put("host", host);
        config.put("port", port);
        config.put("name", "some name");
        config.put("username", username);
        config.put("password", password);
        return config;
    }

    @NotNull
    private Map<String, Object> getConfigUsingChannelName(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put(CHANNEL_NAME_ATTRIBUTE, topic);
        config.put("host", host);
        config.put("port", port);
        config.put("name", "some name");
        config.put("username", username);
        config.put("password", password);
        return config;
    }

}
