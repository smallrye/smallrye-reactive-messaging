package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.extension.MediatorManager;

public class AmqpFailureHandlerTest extends AmqpTestBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    private MyReceiverBean deploy() {
        Weld weld = new Weld();
        weld.addBeanClass(MyReceiverBean.class);

        container = weld.initialize();
        await().until(() -> container.select(MediatorManager.class).get().isInitialized());
        return container.getBeanManager().createInstance().select(MyReceiverBean.class).get();
    }

    @Test
    public void testFailStrategy() {
        getFailConfig();
        MyReceiverBean bean = deploy();
        AtomicInteger counter = new AtomicInteger();

        AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
        await().until(() -> connector.isReady("amqp"));

        usage.produceTenIntegers("fail", counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 4);
        // Other messages should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3);
    }

    @Test
    public void testAcceptStrategy() {
        getAcceptConfig();
        MyReceiverBean bean = deploy();
        AtomicInteger counter = new AtomicInteger();

        AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
        await()
                .atMost(1, TimeUnit.MINUTES)
                .until(() -> connector.isReady("amqp"));

        usage.produceTenIntegers("accept", counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        // All messages should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testReleaseStrategy() {
        getReleaseConfig();
        MyReceiverBean bean = deploy();
        AtomicInteger counter = new AtomicInteger();
        AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
        await().until(() -> connector.isReady("amqp"));

        usage.produceTenIntegers("release", counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 20);
        // All messages should not have been received, after 9 the released message should have been re-delivered again.
        assertThat(bean.list()).contains(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(bean.list()).satisfies(l -> assertThat(l.indexOf(3)).isNotEqualTo(l.lastIndexOf(3)));
        assertThat(bean.list()).satisfies(l -> assertThat(l.indexOf(6)).isNotEqualTo(l.lastIndexOf(6)));
        assertThat(bean.list()).satisfies(l -> assertThat(l.indexOf(9)).isNotEqualTo(l.lastIndexOf(9)));
    }

    @Test
    public void testRejectStrategy() {
        getRejectConfig();
        MyReceiverBean bean = deploy();
        AtomicInteger counter = new AtomicInteger();
        AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
        await().until(() -> connector.isReady("amqp"));

        usage.produceTenIntegers("reject", counter::getAndIncrement);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        // All messages should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    private void getFailConfig() {
        new MapBasedConfig()
                .put("mp.messaging.incoming.amqp.address", "fail")
                .put("mp.messaging.incoming.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.amqp.host", host)
                .put("mp.messaging.incoming.amqp.port", port)
                .put("mp.messaging.incoming.amqp.durable", true)
                .put("amqp-username", username)
                .put("amqp-password", password)
                // fail is the default.
                .write();
    }

    private void getAcceptConfig() {
        new MapBasedConfig()
                .put("mp.messaging.incoming.amqp.address", "accept")
                .put("mp.messaging.incoming.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.amqp.host", host)
                .put("mp.messaging.incoming.amqp.port", port)
                .put("mp.messaging.incoming.amqp.durable", true)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("mp.messaging.incoming.amqp.failure-strategy", "accept")
                .write();
    }

    private void getRejectConfig() {
        new MapBasedConfig()
                .put("mp.messaging.incoming.amqp.address", "reject")
                .put("mp.messaging.incoming.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.amqp.host", host)
                .put("mp.messaging.incoming.amqp.port", port)
                .put("mp.messaging.incoming.amqp.durable", true)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("mp.messaging.incoming.amqp.failure-strategy", "reject")
                .write();
    }

    private void getReleaseConfig() {
        new MapBasedConfig()
                .put("mp.messaging.incoming.amqp.address", "release")
                .put("mp.messaging.incoming.amqp.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.amqp.host", host)
                .put("mp.messaging.incoming.amqp.port", port)
                .put("mp.messaging.incoming.amqp.durable", true)
                .put("amqp-username", username)
                .put("amqp-password", password)
                .put("mp.messaging.incoming.amqp.failure-strategy", "release")
                .write();
    }

    @ApplicationScoped
    public static class MyReceiverBean {
        private List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("amqp")
        public CompletionStage<Void> process(AmqpMessage<Integer> record) {
            Integer payload = record.getPayload();
            received.add(payload);
            if (payload != 0 && payload % 3 == 0) {
                return record.nack(new IllegalArgumentException("nack 3 - " + payload));
            }
            return record.ack();
        }

        public List<Integer> list() {
            return received;
        }

    }
}
