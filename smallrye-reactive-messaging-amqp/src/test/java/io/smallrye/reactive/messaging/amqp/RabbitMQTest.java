package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class RabbitMQTest extends RabbitMQBrokerTestBase {

    private WeldContainer container;

    Weld weld = new Weld();

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.shutdown();
        }

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testSendingMessagesToRabbitMQ() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink",
                v -> latch.countDown());

        weld.addBeanClass(ProducingBean.class);

        new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.address", "sink")
                .with("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.host", host)
                .with("mp.messaging.outgoing.sink.port", port)
                .with("mp.messaging.outgoing.sink.durable", false)
                .with("mp.messaging.outgoing.sink.use-anonymous-sender", false)
                .with("mp.messaging.outgoing.sink.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    public void testSendingMessagesToRabbitMQWithNotAnonymousDetection() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink-not-anonymous",
                v -> latch.countDown());

        weld.addBeanClass(ProducingBean.class);

        new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.address", "sink-not-anonymous")
                .with("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.host", host)
                .with("mp.messaging.outgoing.sink.port", port)
                .with("mp.messaging.outgoing.sink.durable", false)
                .with("mp.messaging.outgoing.sink.tracing-enabled", false)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .write();

        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    public void testReceivingMessagesFromRabbitMQ() {
        new MapBasedConfig()
                .put("mp.messaging.incoming.data.address", "data")
                .put("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.data.host", host)
                .put("mp.messaging.incoming.data.port", port)
                .put("mp.messaging.incoming.data.tracing-enabled", false)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .write();

        weld.addBeanClass(ConsumptionBean.class);

        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));
        ConsumptionBean bean = container.getBeanManager().createInstance().select(ConsumptionBean.class).get();

        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers("data", counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }
}
