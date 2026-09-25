package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class MqttQoS2Test extends MqttTestBase {

    @AfterEach
    public void cleanup() {
        Clients.clear();
    }

    @Test
    public void testSourceWithQoS2() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("channel-name", topic);
        config.put("qos", 2);
        MqttSource source = new MqttSource(vertx, new MqttConnectorIncomingConfiguration(new MapBasedConfig(config)),
                null, null);

        List<MqttMessage<?>> messages = new ArrayList<>();
        Flow.Publisher<? extends MqttMessage<?>> stream = source.getSource();
        Multi.createFrom().publisher(stream).subscribe().with(messages::add);
        awaitUntilReady(source);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(topic, 5, null, counter::getAndIncrement)).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 5);
        assertThat(messages).hasSize(5);
    }

    @Test
    public void testSourceWithQoS2AndV5() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("channel-name", topic);
        config.put("qos", 2);
        config.put("mqtt-version", 5);
        MqttSource source = new MqttSource(vertx, new MqttConnectorIncomingConfiguration(new MapBasedConfig(config)),
                null, null);

        List<MqttMessage<?>> messages = new ArrayList<>();
        Flow.Publisher<? extends MqttMessage<?>> stream = source.getSource();
        Multi.createFrom().publisher(stream).subscribe().with(messages::add);
        awaitUntilReady(source);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(topic, 5, null, counter::getAndIncrement)).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 5);
        assertThat(messages).hasSize(5);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSinkWithQoS2() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(0);
        usage.consumeIntegers(topic, 5, 60, TimeUnit.SECONDS,
                latch::countDown,
                v -> expected.getAndIncrement());

        Map<String, Object> config = new HashMap<>();
        config.put("channel-name", topic);
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("qos", 2);
        MqttSink sink = new MqttSink(vertx, new MqttConnectorOutgoingConfiguration(new MapBasedConfig(config)), null, null);

        Subscriber<? extends Message<?>> subscriber = sink.getSink();
        Multi.createFrom().range(0, 5)
                .map(Message::of)
                .subscribe((Subscriber<? super Message<Integer>>) subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        await().atMost(1, TimeUnit.MINUTES).untilAtomic(expected, is(5));
    }

    @Test
    public void testSourceWithQoS1() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("channel-name", topic);
        config.put("qos", 1);
        MqttSource source = new MqttSource(vertx, new MqttConnectorIncomingConfiguration(new MapBasedConfig(config)),
                null, null);

        List<MqttMessage<?>> messages = new ArrayList<>();
        Flow.Publisher<? extends MqttMessage<?>> stream = source.getSource();
        Multi.createFrom().publisher(stream).subscribe().with(messages::add);
        awaitUntilReady(source);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(topic, 5, null, counter::getAndIncrement)).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 5);
        assertThat(messages).hasSize(5);
    }
}
