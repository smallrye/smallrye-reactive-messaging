package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.smallrye.reactive.messaging.test.common.config.SmallRyeConfigTestUtil;

public class GracefulShutdownTest extends RabbitMQBrokerTestBase {

    private WeldContainer container;

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.shutdown();
        }
        MapBasedConfig.cleanup();
        SmallRyeConfigTestUtil.releaseConfig();
    }

    @Test
    public void testGracefulShutdownDrainsInFlightMessages() {
        String routingKey = "normal";

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.exchange.name", exchangeName)
                .with("mp.messaging.incoming.data.exchange.declare", true)
                .with("mp.messaging.incoming.data.queue.name", queueName)
                .with("mp.messaging.incoming.data.queue.declare", true)
                .with("mp.messaging.incoming.data.routing-keys", routingKey)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.tracing.enabled", false)
                .with("mp.messaging.incoming.data.graceful-shutdown", true)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .write();

        Weld weld = new Weld();
        weld.addBeanClass(SlowRabbitMQConsumerBean.class);
        SmallRyeConfigTestUtil.installConfig();
        container = weld.initialize();

        await().until(() -> isRabbitMQConnectorAlive(container));
        await().until(() -> isRabbitMQConnectorReady(container));

        SlowRabbitMQConsumerBean bean = container.select(SlowRabbitMQConsumerBean.class).get();
        List<String> received = bean.getReceived();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers(exchangeName, queueName, routingKey, counter::getAndIncrement);

        await().atMost(30, TimeUnit.SECONDS).until(() -> received.size() >= 3);

        int countBeforeShutdown = received.size();

        container.shutdown();
        container = null;

        int countAfterShutdown = received.size();
        assertThat(countAfterShutdown).isGreaterThanOrEqualTo(countBeforeShutdown);
    }

    @ApplicationScoped
    public static class SlowRabbitMQConsumerBean {

        private final List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        @Blocking
        public void consume(String payload) throws InterruptedException {
            Thread.sleep(100);
            received.add(payload);
        }

        public List<String> getReceived() {
            return received;
        }
    }
}
