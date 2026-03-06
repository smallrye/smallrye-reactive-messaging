package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.jboss.weld.exceptions.DeploymentException;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.ConnectionFactory;

import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class RabbitMQSourceCDIConfigTest extends WeldTestBase {

    @Test
    public void testConfigByCDIMissingBean() {
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.queue.name", "data")
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("mp.messaging.incoming.data.client-options-name", "myclientoptions")
                .with("mp.messaging.incoming.data.tracing.enabled", false)
                .write();

        assertThatThrownBy(() -> container = weld.initialize())
                .isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigByCDIIncorrectBean() {
        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.queue.name", "data")
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("mp.messaging.incoming.data.client-options-name", "dummyoptionsnonexistent")
                .with("mp.messaging.incoming.data.tracing.enabled", false)
                .write();

        assertThatThrownBy(() -> container = weld.initialize())
                .isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigByCDICorrect() {
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
                .with("mp.messaging.incoming.data.tracing.enabled", false)
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
        String queueName = UUID.randomUUID().toString();
        weld.addBeanClass(ClientConfigurationBean.class);
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.queue.name", queueName)
                .with("mp.messaging.incoming.data.exchange.name", queueName)
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.tracing.enabled", false)
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
        weld.addBeanClass(ConsumptionBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.queue.name", "data")
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.tracing.enabled", false)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("rabbitmq-client-options-name", "myclientoptions")
                .write();

        assertThatThrownBy(() -> container = weld.initialize())
                .isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigGlobalOptionsByCDIIncorrectBean() {
        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.queue.name", "data")
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.tracing.enabled", false)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("rabbitmq-client-options-name", "dummyoptionsnonexistent")
                .write();

        assertThatThrownBy(() -> container = weld.initialize())
                .isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigInterceptor() {
        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(MyClientCustomizer.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.queue.name", "data")
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.tracing.enabled", false)
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
        usage.produceTenIntegers("data", "data", "", counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @ApplicationScoped
    public static class MyClientCustomizer implements ClientCustomizer<ConnectionFactory> {

        @Override
        public ConnectionFactory customize(String channel, Config channelConfig, ConnectionFactory config) {
            assertThat(config.getHost()).isEqualTo(System.getProperty("rabbitmq-host"));
            assertThat(config.getPort()).isEqualTo(Integer.parseInt(System.getProperty("rabbitmq-port")));
            return config;
        }
    }

}
