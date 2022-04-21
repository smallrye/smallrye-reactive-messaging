package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class FailureHandlerTest extends MqttTestBase {

    private WeldContainer container;

    @AfterEach
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
        return container.getBeanManager().createInstance().select(MyReceiverBean.class).get();
    }

    @Test
    public void testFailStrategy() {
        getFailConfig();
        MyReceiverBean bean = deploy();
        AtomicInteger counter = new AtomicInteger();

        MqttConnector connector = container.getBeanManager().createInstance().select(MqttConnector.class,
                ConnectorLiteral.of(MqttConnector.CONNECTOR_NAME)).get();
        await().until(connector::isReady);

        usage.produceStrings("fail", 10, null, () -> Integer.toString(counter.getAndIncrement()));

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 4);
        // Other messages should not have been received.
        assertThat(bean.list()).containsExactly("0", "1", "2", "3");
    }

    @Test
    public void testIgnoreStrategy() {
        getIgnoreConfig();
        MyReceiverBean bean = deploy();
        AtomicInteger counter = new AtomicInteger();

        MqttConnector connector = container.getBeanManager().createInstance().select(MqttConnector.class,
                ConnectorLiteral.of(MqttConnector.CONNECTOR_NAME)).get();
        await().until(connector::isReady);

        usage.produceStrings("ignore", 10, null, () -> Integer.toString(counter.getAndIncrement()));

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        // All messages should not have been received.
        assertThat(bean.list()).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

    }

    private void getFailConfig() {
        new MapBasedConfig()
                .put("mp.messaging.incoming.mqtt.topic", "fail")
                .put("mp.messaging.incoming.mqtt.connector", MqttConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.mqtt.host", address)
                .put("mp.messaging.incoming.mqtt.port", port)
                .put("mp.messaging.incoming.mqtt.durable", true)
                // fail is the default.
                .write();
    }

    private void getIgnoreConfig() {
        new MapBasedConfig()
                .put("mp.messaging.incoming.mqtt.topic", "ignore")
                .put("mp.messaging.incoming.mqtt.connector", MqttConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.mqtt.host", address)
                .put("mp.messaging.incoming.mqtt.port", port)
                .put("mp.messaging.incoming.mqtt.durable", true)
                .put("mp.messaging.incoming.mqtt.failure-strategy", "ignore")
                .write();
    }

    @ApplicationScoped
    public static class MyReceiverBean {
        private final List<String> received = new CopyOnWriteArrayList<>();

        private static final List<String> SKIPPED = Arrays.asList("3", "6", "9");

        @Incoming("mqtt")
        public CompletionStage<Void> process(MqttMessage<byte[]> message) {
            String payload = new String(message.getPayload());
            received.add(payload);
            if (SKIPPED.contains(payload)) {
                return message.nack(new IllegalArgumentException("nack 3 - " + payload));
            }
            return message.ack();
        }

        public List<String> list() {
            return received;
        }

    }
}
