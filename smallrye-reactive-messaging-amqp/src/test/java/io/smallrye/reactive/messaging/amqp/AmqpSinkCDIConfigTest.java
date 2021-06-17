package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.jboss.weld.exceptions.DeploymentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class AmqpSinkCDIConfigTest extends AmqpBrokerTestBase {

    private WeldContainer container;
    private AmqpConnector provider;

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

        weld.addBeanClass(ProducingBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)

                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("mp.messaging.outgoing.sink.client-options-name", "myclientoptions")
                .put("mp.messaging.outgoing.sink.tracing-enabled", false)
                .write();

        assertThatThrownBy(() -> container = weld.initialize()).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigByCDIIncorrectBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ProducingBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("mp.messaging.outgoing.sink.client-options-name", "dummyoptionsnonexistent")
                .put("mp.messaging.outgoing.sink.tracing-enabled", false)
                .write();

        assertThatThrownBy(() -> container = weld.initialize()).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigByCDICorrect() throws InterruptedException {
        Weld weld = new Weld();

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink",
                v -> latch.countDown());

        weld.addBeanClass(ProducingBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("mp.messaging.outgoing.sink.durable", false)
                .put("mp.messaging.outgoing.sink.tracing-enabled", false)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("mp.messaging.outgoing.sink.client-options-name", "myclientoptions")
                .write();

        container = weld.initialize();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    @Disabled("Failing on CI - need to be investigated")
    public void testConfigGlobalOptionsByCDIMissingBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ProducingBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("mp.messaging.outgoing.sink.tracing-enabled", false)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("amqp-client-options-name", "dummyoptionsnonexistent")
                .write();

        assertThatThrownBy(() -> {
            container = weld.initialize();
        }).isInstanceOf(DeploymentException.class);
    }

    @Test
    @Disabled("Failing on CI - to be investigated")
    public void testConfigGlobalOptionsByCDIIncorrectBean() {
        Weld weld = new Weld();

        weld.addBeanClass(ProducingBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("mp.messaging.outgoing.sink.durable", false)
                .put("mp.messaging.outgoing.sink.tracing-enabled", false)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("amqp-client-options-name", "dummyoptionsnonexistent")
                .write();

        assertThatThrownBy(() -> {
            container = weld.initialize();
        }).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testConfigGlobalOptionsByCDICorrect() throws InterruptedException {
        Weld weld = new Weld();

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink",
                v -> latch.countDown());

        weld.addBeanClass(ProducingBean.class);
        weld.addBeanClass(ClientConfigurationBean.class);

        new MapBasedConfig()
                .put("mp.messaging.outgoing.sink.address", "sink")
                .put("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.sink.host", host)
                .put("mp.messaging.outgoing.sink.port", port)
                .put("mp.messaging.outgoing.sink.durable", false)
                .put("mp.messaging.outgoing.sink.tracing-enabled", false)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("amqp-client-options-name", "myclientoptions")
                .write();

        container = weld.initialize();

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }
}
