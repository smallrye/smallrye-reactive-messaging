package io.smallrye.reactive.messaging.mqtt.server;

import static io.netty.handler.codec.mqtt.MqttQoS.*;
import static org.awaitility.Awaitility.await;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mutiny.core.Vertx;

@ExtendWith(VertxExtension.class)
class MqttServerSourceTest {

    @Test
    void testSingle(io.vertx.core.Vertx vertx, VertxTestContext testContext) {
        final Map<String, String> configMap = new HashMap<>();
        configMap.put("port", "0");
        final MqttServerSource source = new MqttServerSource(new Vertx(vertx),
                new MqttServerConnectorIncomingConfiguration(TestUtils.config(configMap)));
        final PublisherBuilder<MqttMessage> mqttMessagePublisherBuilder = source.source();
        final TestMqttMessage testMessage = new TestMqttMessage("hello/topic", 1, "Hello world!",
                EXACTLY_ONCE.value(), false);
        final Checkpoint messageReceived = testContext.checkpoint();
        final Checkpoint messageAcknowledged = testContext.checkpoint();

        mqttMessagePublisherBuilder.forEach(mqttMessage -> {
            testContext.verify(() -> TestUtils.assertMqttEquals(testMessage, mqttMessage));
            messageReceived.flag();
            mqttMessage.ack().thenApply(aVoid -> {
                messageAcknowledged.flag();
                return aVoid;
            });
        }).run();
        TestUtils.sendMqttMessages(Collections.singletonList(testMessage),
                CompletableFuture.supplyAsync(() -> {
                    await().until(source::port, port -> port != 0);
                    return source.port();
                }), testContext);
    }

    @Test
    void testMultiple(io.vertx.core.Vertx vertx, VertxTestContext testContext) {
        final Map<String, String> configMap = new HashMap<>();
        configMap.put("port", "0");
        final MqttServerSource source = new MqttServerSource(new Vertx(vertx),
                new MqttServerConnectorIncomingConfiguration(TestUtils.config(configMap)));
        final PublisherBuilder<MqttMessage> mqttMessagePublisherBuilder = source.source();
        final List<TestMqttMessage> testMessages = new CopyOnWriteArrayList<>();
        testMessages
                .add(new TestMqttMessage("hello/topic", 1, "Hello world!", EXACTLY_ONCE.value(), false));
        testMessages
                .add(new TestMqttMessage("foo/bar", 2, "dkufhdspkjfosdjfs;", AT_LEAST_ONCE.value(), true));
        testMessages
                .add(new TestMqttMessage("foo/bar", -1, "Hello world!", AT_MOST_ONCE.value(), false));
        final Checkpoint messageReceived = testContext.checkpoint(testMessages.size());
        final Checkpoint messageAcknowledged = testContext.checkpoint(testMessages.size());
        final AtomicInteger index = new AtomicInteger(0);

        mqttMessagePublisherBuilder.forEach(mqttMessage -> {
            testContext.verify(
                    () -> TestUtils.assertMqttEquals(testMessages.get(index.getAndIncrement()), mqttMessage));
            messageReceived.flag();
            mqttMessage.ack().thenApply(aVoid -> {
                messageAcknowledged.flag();
                return aVoid;
            });
        }).run();
        TestUtils.sendMqttMessages(testMessages,
                CompletableFuture.supplyAsync(() -> {
                    await().until(source::port, port -> port != 0);
                    return source.port();
                }), testContext);
    }
}
