package io.smallrye.reactive.messaging.mqtt;

import static org.awaitility.Awaitility.await;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.jboss.logging.Logger;

/**
 * MQTT v5 test utility using Paho v5 client.
 */
public class MqttV5Usage {

    private static final Logger LOGGER = Logger.getLogger(MqttV5Usage.class);
    private final MqttClient client;

    public MqttV5Usage(String host, int port) {
        try {
            client = new MqttClient("tcp://" + host + ":" + port, UUID.randomUUID().toString());
            MqttConnectionOptions options = new MqttConnectionOptions();
            options.setCleanStart(true);
            client.connect(options);
            await().until(client::isConnected);
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Publish a message with MQTT v5 properties.
     */
    public void publish(String topic, byte[] payload, int qos, boolean retained, MqttProperties properties) {
        try {
            MqttMessage msg = new MqttMessage(payload);
            msg.setQos(qos);
            msg.setRetained(retained);
            if (properties != null) {
                msg.setProperties(properties);
            }
            client.publish(topic, msg);
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Publish a simple message with v5 properties.
     */
    public void produce(String topic, int messageCount, Runnable completionCallback, Supplier<byte[]> messageSupplier,
            MqttProperties properties) {
        Thread t = new Thread(() -> {
            LOGGER.infof("Starting MQTT v5 client to write %s messages", messageCount);
            try {
                for (int i = 0; i != messageCount; ++i) {
                    byte[] payload = messageSupplier.get();
                    MqttMessage msg = new MqttMessage(payload);
                    msg.setQos(0);
                    if (properties != null) {
                        msg.setProperties(properties);
                    }
                    client.publish(topic, msg);
                }
            } catch (Exception e) {
                LOGGER.error("Unable to send v5 message", e);
            } finally {
                if (completionCallback != null) {
                    completionCallback.run();
                }
            }
        });
        t.setName(topic + "-v5-thread");
        t.start();
    }

    public void produce(String topic, int messageCount, Runnable completionCallback, Supplier<byte[]> messageSupplier) {
        produce(topic, messageCount, completionCallback, messageSupplier, null);
    }

    /**
     * Consume messages with v5 properties.
     */
    public void consumeRaw(String topic, int count, long timeout, TimeUnit unit, Runnable completion,
            Consumer<MqttMessage> messageConsumer) {
        AtomicLong readCounter = new AtomicLong();
        CountDownLatch subscribed = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            LOGGER.infof("Starting v5 consumer to read messages on %s", topic);
            try {
                client.subscribe(topic, 0, (topicName, msg) -> {
                    LOGGER.infof("V5 Consumer %s: consuming message", topic);
                    messageConsumer.accept(msg);
                    readCounter.incrementAndGet();
                    if (readCounter.get() >= count) {
                        client.unsubscribe(topic);
                    }
                });
                subscribed.countDown();
            } catch (Exception e) {
                LOGGER.errorf("Unable to receive v5 messages from %s", topic, e);
            } finally {
                if (completion != null) {
                    completion.run();
                }
            }
        });
        t.setName(topic + "-v5-thread");
        t.start();
        try {
            subscribed.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void consumeStrings(String topic, int count, long timeout, TimeUnit unit, Runnable completion,
            Consumer<String> consumer) {
        consumeRaw(topic, count, timeout, unit, completion, msg -> consumer.accept(new String(msg.getPayload())));
    }

    public void close() {
        try {
            if (client.isConnected()) {
                client.disconnect();
            }
            client.close();
        } catch (MqttException e) {
            LOGGER.error("Unable to close the MQTT v5 client", e);
        } catch (RejectedExecutionException e) {
            // Ignore.
        }
    }
}
