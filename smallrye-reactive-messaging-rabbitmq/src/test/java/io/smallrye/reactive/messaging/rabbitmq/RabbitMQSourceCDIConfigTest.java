package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.jboss.weld.exceptions.DeploymentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class RabbitMQSourceCDIConfigTest extends RabbitMQBrokerTestBase {

    private WeldContainer container;

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.shutdown();
        }

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());

        System.clearProperty("mp-config");
        System.clearProperty("client-options-name");
        System.clearProperty("rabbitmq-client-options-name");
    }

    @Test
    public void testConfigByCDIMissingBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(ExecutionHolder.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.queue.name", "data")
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("mp.messaging.incoming.data.client-options-name", "myclientoptions")
                .with("mp.messaging.incoming.data.tracing-enabled", false)
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
                .with("mp.messaging.incoming.data.queue.name", "data")
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("mp.messaging.incoming.data.client-options-name", "dummyoptionsnonexistent")
                .with("mp.messaging.incoming.data.tracing-enabled", false)
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
                .with("mp.messaging.incoming.data.queue.name", "data")
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("mp.messaging.incoming.data.client-options-name", "myclientoptions")
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .write();

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAlive(container));
        await().until(() -> isRabbitMQConnectorReady(container));
        List<Integer> list = container.select(ConsumptionBean.class).get().getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers("data", "data", "", counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testConfigGlobalOptionsByCDICorrect() {
        Weld weld = new Weld();

        String queueName = UUID.randomUUID().toString();
        weld.addBeanClass(ClientConfigurationBean.class);
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.queue.name", queueName)
                .with("mp.messaging.incoming.data.exchange.name", queueName)
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("rabbitmq-client-options-name", "myclientoptions")
                .write();

        container = weld.initialize();
        await().until(() -> isRabbitMQConnectorAlive(container));
        await().until(() -> isRabbitMQConnectorReady(container));
        List<Integer> list = container.select(ConsumptionBean.class).get().getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers(queueName, queueName, "", counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testConfigGlobalOptionsByCDIMissingBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(ExecutionHolder.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.queue.name", "data")
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("rabbitmq-client-options-name", "myclientoptions")
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
                .with("mp.messaging.incoming.data.queue.name", "data")
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("rabbitmq-client-options-name", "dummyoptionsnonexistent")
                .write();

        assertThatThrownBy(() -> container = weld.initialize())
                .isInstanceOf(DeploymentException.class);
    }
}
