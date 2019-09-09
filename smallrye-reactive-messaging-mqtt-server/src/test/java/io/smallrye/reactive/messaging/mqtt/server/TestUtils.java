package io.smallrye.reactive.messaging.mqtt.server;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.config.Config;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.Assertions;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxTestContext;

class TestUtils {

    static Config config(Map<String, String> map) {
        return new SmallRyeConfigBuilder()
                .withSources(new PropertiesConfigSource(map, "", 0)).build();
    }

    static void sendMqttMessages(List<TestMqttMessage> messages,
            CompletableFuture<Integer> futurePort,
            VertxTestContext testContext) {
        Checkpoint messageSent = testContext.checkpoint((int) messages.size());
        Checkpoint clientClosed = testContext.checkpoint();
        futurePort.thenAccept(port -> new Thread(() -> {
            try {
                final MqttClient mqttClient = new MqttClient("tcp://localhost:" + port,
                        MqttClient.generateClientId());
                mqttClient.connect();
                messages.forEach(message -> {
                    try {
                        MqttMessage mqttMessage = new MqttMessage(message.getBody().getBytes());
                        mqttMessage.setQos(message.getQos());
                        mqttMessage.setRetained(message.isRetained());
                        mqttMessage.setId(message.getId());
                        mqttClient.publish(message.getTopic(), mqttMessage);
                        messageSent.flag();
                    } catch (MqttException e) {
                        testContext.failNow(e);
                    }
                });
                mqttClient.disconnect();
                mqttClient.close();
                clientClosed.flag();
            } catch (MqttException e) {
                testContext.failNow(e);
            }
        }).start());
    }

    static void assertMqttEquals(TestMqttMessage expected, io.smallrye.reactive.messaging.mqtt.server.MqttMessage message) {
        Assertions.assertEquals(expected.getId(), message.getMessageId());
        Assertions.assertEquals(expected.getTopic(), message.getTopic());
        Assertions.assertEquals(expected.getBody(), new String(message.getPayload()));
        Assertions.assertEquals(expected.isRetained(), message.isRetain());
        Assertions.assertFalse(message.isDuplicate());
    }

    static Subscriber<io.smallrye.reactive.messaging.mqtt.server.MqttMessage> createSubscriber(VertxTestContext testContext,
            AtomicBoolean opened, List<TestMqttMessage> expectedMessages) {
        return new Subscriber<io.smallrye.reactive.messaging.mqtt.server.MqttMessage>() {
            Subscription sub;
            AtomicInteger index = new AtomicInteger(0);
            Checkpoint messageReceived;
            Checkpoint messageAcknowledged;

            @Override
            public void onSubscribe(Subscription s) {
                this.sub = s;
                this.messageReceived = testContext.checkpoint(expectedMessages.size());
                this.messageAcknowledged = testContext.checkpoint(expectedMessages.size());
                sub.request(5);
                opened.set(true);
            }

            @Override
            public void onNext(io.smallrye.reactive.messaging.mqtt.server.MqttMessage message) {
                testContext.verify(() -> TestUtils.assertMqttEquals(expectedMessages.get(index.getAndIncrement()), message));
                messageReceived.flag();
                message.ack().thenAccept(v -> messageAcknowledged.flag());
                sub.request(1);
            }

            @Override
            public void onError(Throwable t) {
                testContext.failNow(t);
            }

            @Override
            public void onComplete() {
                // Do nothing.
            }
        };
    }
}
