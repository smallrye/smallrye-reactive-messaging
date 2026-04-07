package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;

import java.nio.charset.StandardCharsets;
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
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class MqttV5SinkTest extends MqttTestBase {

    @AfterEach
    public void cleanup() {
        Clients.clear();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSinkWithV5PropertiesSendsMessages() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger received = new AtomicInteger(0);

        // Use v3 consumer to verify messages arrive (broker handles protocol translation)
        usage.consumeStrings(topic, 1, 30, TimeUnit.SECONDS,
                latch::countDown,
                v -> received.getAndIncrement());

        Map<String, Object> config = new HashMap<>();
        config.put("channel-name", topic);
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("mqtt-version", 5);
        MqttSink sink = new MqttSink(vertx, new MqttConnectorOutgoingConfiguration(new MapBasedConfig(config)), null, null);

        SendingMqttMessageMetadata v5Meta = SendingMqttMessageMetadataBuilder.builder()
                .withContentType("text/plain")
                .withResponseTopic("reply/" + topic)
                .withCorrelationData("req-001".getBytes(StandardCharsets.UTF_8))
                .withMessageExpiryInterval(120)
                .withPayloadFormatIndicator(1)
                .withUserProperty("app-id", "test-app")
                .build();

        Subscriber<? extends Message<?>> subscriber = sink.getSink();
        Multi.createFrom().item("hello-v5")
                .map(s -> Message.of(s, Metadata.of(v5Meta)))
                .subscribe((Subscriber<? super Message<String>>) subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        await().untilAtomic(received, is(1));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSinkWithMetadataTopicOverride() throws InterruptedException {
        String configTopic = UUID.randomUUID().toString();
        String actualTopic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger received = new AtomicInteger(0);

        // Listen on the actual topic (not the config topic)
        usage.consumeStrings(actualTopic, 1, 30, TimeUnit.SECONDS,
                latch::countDown,
                v -> received.getAndIncrement());

        Map<String, Object> config = new HashMap<>();
        config.put("channel-name", configTopic);
        config.put("topic", configTopic);
        config.put("host", address);
        config.put("port", port);
        MqttSink sink = new MqttSink(vertx, new MqttConnectorOutgoingConfiguration(new MapBasedConfig(config)), null, null);

        // Override topic via metadata
        SendingMqttMessageMetadata meta = new SendingMqttMessageMetadata(actualTopic, MqttQoS.AT_MOST_ONCE, false);

        Subscriber<? extends Message<?>> subscriber = sink.getSink();
        Multi.createFrom().item("topic-override")
                .map(s -> Message.of(s, Metadata.of(meta)))
                .subscribe((Subscriber<? super Message<String>>) subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        await().untilAtomic(received, is(1));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSinkWithMetadataQoSOverride() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger received = new AtomicInteger(0);

        usage.consumeStrings(topic, 1, 30, TimeUnit.SECONDS,
                latch::countDown,
                v -> received.getAndIncrement());

        Map<String, Object> config = new HashMap<>();
        config.put("channel-name", topic);
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        config.put("qos", 0); // Default QoS 0
        MqttSink sink = new MqttSink(vertx, new MqttConnectorOutgoingConfiguration(new MapBasedConfig(config)), null, null);

        // Override QoS to 1 via metadata
        SendingMqttMessageMetadata meta = new SendingMqttMessageMetadata(null, MqttQoS.AT_LEAST_ONCE, false);

        Subscriber<? extends Message<?>> subscriber = sink.getSink();
        Multi.createFrom().item("qos-override")
                .map(s -> Message.of(s, Metadata.of(meta)))
                .subscribe((Subscriber<? super Message<String>>) subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        await().untilAtomic(received, is(1));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSinkWithMetadataRetainOverride() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        List<MqttMessage> received = new CopyOnWriteArrayList<>();

        usage.consumeRaw(topic, 1, 30, TimeUnit.SECONDS,
                latch::countDown,
                (top, msg) -> received.add(msg));

        Map<String, Object> config = new HashMap<>();
        config.put("channel-name", topic);
        config.put("topic", topic);
        config.put("host", address);
        config.put("port", port);
        MqttSink sink = new MqttSink(vertx, new MqttConnectorOutgoingConfiguration(new MapBasedConfig(config)), null, null);

        // Set retain via metadata
        SendingMqttMessageMetadata meta = new SendingMqttMessageMetadata(null, null, true);

        Subscriber<? extends Message<?>> subscriber = sink.getSink();
        Multi.createFrom().item("retain-test")
                .map(s -> Message.of(s, Metadata.of(meta)))
                .subscribe((Subscriber<? super Message<String>>) subscriber);

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
        await().untilAsserted(() -> assertThat(received).hasSize(1));
    }
}
