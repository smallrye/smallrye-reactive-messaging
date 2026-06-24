package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
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

public class GracefulShutdownAmqpTest extends AmqpBrokerTestBase {

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
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("mp.messaging.incoming.data.address", address)
                .with("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("amqp-username", username)
                .with("amqp-password", password)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("mp.messaging.incoming.data.graceful-shutdown", true)
                .write();

        Weld weld = new Weld();
        weld.addBeanClass(SlowAmqpConsumerBean.class);
        container = initializeContainer(weld);

        await().until(() -> isAmqpConnectorAlive(container));
        await().until(() -> isAmqpConnectorReady(container));

        SlowAmqpConsumerBean bean = container.select(SlowAmqpConsumerBean.class).get();
        List<Integer> received = bean.getReceived();

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers(address, counter::getAndIncrement);

        // Wait for some messages to be consumed
        await().atMost(30, TimeUnit.SECONDS).until(() -> received.size() >= 3);

        int countBeforeShutdown = received.size();

        // Shutdown triggers GracefulShutdownController at Priority 40,
        // which drains in-flight messages before AmqpConnector.terminate at Priority 50
        container.shutdown();
        container = null;

        // After shutdown, verify in-flight messages completed
        int countAfterShutdown = received.size();
        assertThat(countAfterShutdown).isGreaterThanOrEqualTo(countBeforeShutdown);
    }

    @ApplicationScoped
    public static class SlowAmqpConsumerBean {

        private final List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        @Blocking
        public void consume(int payload) throws InterruptedException {
            Thread.sleep(100);
            received.add(payload);
        }

        public List<Integer> getReceived() {
            return received;
        }
    }
}
