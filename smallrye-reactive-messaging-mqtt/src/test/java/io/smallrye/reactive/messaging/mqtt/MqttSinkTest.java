package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import repeat.Repeat;

public class MqttSinkTest extends MqttTestBase {

    private WeldContainer container;

    @After
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
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        MqttSink sink = new MqttSink(vertx, new MqttConnectorOutgoingConfiguration(new MapBasedConfig(config)));

        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        Flowable.range(0, 10)
                .map(Message::of)
                .subscribe((Subscriber<? super Message<Integer>>) subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSinkUsingChannelName() throws InterruptedException {
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
        MqttSink sink = new MqttSink(vertx, new MqttConnectorOutgoingConfiguration(new MapBasedConfig(config)));

        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        Flowable.range(0, 10)
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
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        MqttSink sink = new MqttSink(vertx, new MqttConnectorOutgoingConfiguration(new MapBasedConfig(config)));

        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        Flowable.range(0, 10)
                .map(i -> Integer.toString(i))
                .map(Message::of)
                .subscribe((Subscriber<? super Message<String>>) subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        await().untilAtomic(expected, is(10));
        assertThat(expected).hasValue(10);
    }

    @Test
    @Repeat(times = 5)
    public void testABeanProducingMessagesSentToMQTT() throws InterruptedException {
        Clients.clear();
        Weld weld = baseWeld(getConfig());
        weld.addBeanClass(ProducingBean.class);

        CountDownLatch latch = new CountDownLatch(10);
        usage.consumeIntegers("sink", 10, 10, TimeUnit.SECONDS,
                null,
                v -> latch.countDown());

        container = weld.initialize();
        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
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
