package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class MqttAcknowledgementTest extends MqttTestBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        Clients.clear();
    }

    @Test(timeout = 10000)
    public void testAcknowledgmentWithQoS1AndPayloads() throws InterruptedException {
        Weld weld = baseWeld(getConfig(1));
        weld.addBeanClass(EmitterBean.class);

        CountDownLatch latch = new CountDownLatch(2);
        usage.consumeStrings("test-topic-mqtt", 2, 10, TimeUnit.SECONDS,
                null,
                v -> latch.countDown());

        container = weld.initialize();

        EmitterBean bean = container.getBeanManager().createInstance().select(EmitterBean.class).get();
        bean.sendAndAwait();
        bean.sendMessageAndAwait();
        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test(timeout = 10000)
    public void testAcknowledgmentWithQoS0AndPayloads() throws InterruptedException {
        Weld weld = baseWeld(getConfig(0));
        weld.addBeanClass(EmitterBean.class);

        CountDownLatch latch = new CountDownLatch(2);
        usage.consumeStrings("test-topic-mqtt", 2, 10, TimeUnit.SECONDS,
                null,
                v -> latch.countDown());

        container = weld.initialize();

        EmitterBean bean = container.getBeanManager().createInstance().select(EmitterBean.class).get();
        bean.sendAndAwait();
        bean.sendMessageAndAwait();
        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test(timeout = 10000)
    public void testAcknowledgmentWithQoS2AndPayloads() throws InterruptedException {
        Weld weld = baseWeld(getConfig(0));
        weld.addBeanClass(EmitterBean.class);

        CountDownLatch latch = new CountDownLatch(2);
        usage.consumeStrings("test-topic-mqtt", 2, 10, TimeUnit.SECONDS,
                null,
                v -> latch.countDown());

        container = weld.initialize();

        EmitterBean bean = container.getBeanManager().createInstance().select(EmitterBean.class).get();
        bean.sendAndAwait();
        bean.sendMessageAndAwait();
        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    }

    @ApplicationScoped
    public static class EmitterBean {
        @Inject
        @Channel("test-topic-mqtt")
        Emitter<String> mqttEmitter;

        private int counter = 0;

        public void sendAndAwait() {
            mqttEmitter.send("hello-" + counter++).toCompletableFuture().join();
        }

        public void sendMessageAndAwait() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(1);
            mqttEmitter.send(Message.of("hello-message-" + counter++, () -> {
                latch.countDown();
                return CompletableFuture.completedFuture(null);
            }));

            assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        }

    }

    private MapBasedConfig getConfig(int qos) {
        String prefix = "mp.messaging.outgoing.test-topic-mqtt.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "topic", "test-topic-mqtt");
        config.put(prefix + "connector", MqttConnector.CONNECTOR_NAME);
        config.put(prefix + "host", System.getProperty("mqtt-host"));
        config.put(prefix + "port", Integer.valueOf(System.getProperty("mqtt-port")));
        config.put(prefix + "qos", qos);
        if (System.getProperty("mqtt-user") != null) {
            config.put(prefix + "username", System.getProperty("mqtt-user"));
            config.put(prefix + "password", System.getProperty("mqtt-pwd"));
        }
        return new MapBasedConfig(config);
    }

}
