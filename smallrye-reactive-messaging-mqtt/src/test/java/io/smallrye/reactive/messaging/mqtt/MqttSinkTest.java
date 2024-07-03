package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class MqttSinkTest extends MqttTestBase {

    private WeldContainer container;

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        Clients.clear();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSinkUsingInteger() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                v -> expected.getAndIncrement());

        Map<String, Object> config = new HashMap<>();
        config.put("channel-name", topic);
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        MqttSink sink = new MqttSink(vertx, new MqttConnectorOutgoingConfiguration(new MapBasedConfig(config)), null);

        Subscriber<? extends Message<?>> subscriber = sink.getSink();
        Multi.createFrom().range(0, 10)
                .map(Message::of)
                .subscribe((Subscriber<? super Message<Integer>>) subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSinkUsingIntegerAndChannelNameAsTopic() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                v -> expected.getAndIncrement());

        Map<String, Object> config = new HashMap<>();
        config.put("channel-name", topic);
        config.put("host", address);
        config.put("port", port);
        MqttSink sink = new MqttSink(vertx, new MqttConnectorOutgoingConfiguration(new MapBasedConfig(config)), null);

        Subscriber<? extends Message<?>> subscriber = sink.getSink();
        Multi.createFrom().range(0, 10)
                .map(Message::of)
                .subscribe((Subscriber<? super Message<Integer>>) subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSinkUsingString() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeStrings(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                v -> expected.getAndIncrement());

        Map<String, Object> config = new HashMap<>();
        config.put("channel-name", topic);
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        MqttSink sink = new MqttSink(vertx, new MqttConnectorOutgoingConfiguration(new MapBasedConfig(config)), null);

        Subscriber<? extends Message<?>> subscriber = sink.getSink();
        Multi.createFrom().range(0, 10)
                .map(i -> Integer.toString(i))
                .map(Message::of)
                .subscribe((Subscriber<? super Message<String>>) subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);
    }

    @Test
    public void testABeanProducingMessagesSentToMQTT() throws InterruptedException {

        Weld weld = baseWeld(getConfig());
        weld.addBeanClass(ProducingBean.class);

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink", 10, 10, TimeUnit.SECONDS,
                null,
                v -> latch.countDown());

        container = weld.initialize();

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

    }

    @Test
    public void testABeanProducingNullPayloadsSentToMQTT() throws InterruptedException {

        Weld weld = baseWeld(getConfig());
        weld.addBeanClass(NullProducingBean.class);

        CountDownLatch latch = new CountDownLatch(1);
        usage.consumeStrings("sink", 10, 10, TimeUnit.SECONDS, latch::countDown, v -> {
        });

        container = weld.initialize();

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

    }

    @Test
    public void testSinkUsingRawWithRetain() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        List<MqttMessage> expected = new CopyOnWriteArrayList<>();
        usage.consumeRaw(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (top, msg) -> expected.add(msg));

        Map<String, Object> config = new HashMap<>();
        config.put("channel-name", topic);
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("retain", true);
        MqttSink sink = new MqttSink(vertx, new MqttConnectorOutgoingConfiguration(new MapBasedConfig(config)), null);

        Subscriber<? extends Message<?>> subscriber = sink.getSink();
        Multi.createFrom().range(1_234, 1_244)
                .map(Message::of)
                .subscribe((Subscriber<? super Message<Integer>>) subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        await().untilAsserted(() -> assertThat(expected).hasSize(10)
                .allSatisfy(m -> assertThat(m.isRetained()).isFalse()));

        List<MqttMessage> expectedRetained = new CopyOnWriteArrayList<>();
        usage.consumeRaw(topic, 10, 10, TimeUnit.SECONDS,
                latch::countDown,
                (top, msg) -> expectedRetained.add(msg));

        await().pollDelay(2, TimeUnit.SECONDS).untilAsserted(() -> assertThat(expectedRetained).hasSize(1)
                .allSatisfy(m -> assertThat(m.isRetained()).isTrue()));

    }

    private MapBasedConfig getConfig() {
        String prefix = "mp.messaging.outgoing.sink.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "topic", "sink");
        config.put(prefix + "connector", MqttConnector.CONNECTOR_NAME);
        config.put(prefix + "host", System.getProperty("mqtt-host"));
        config.put(prefix + "port", Integer.valueOf(System.getProperty("mqtt-port")));
        if (System.getProperty("mqtt-user") != null) {
            config.put(prefix + "username", System.getProperty("mqtt-user"));
            config.put(prefix + "password", System.getProperty("mqtt-pwd"));
        }
        return new MapBasedConfig(config);
    }

}
