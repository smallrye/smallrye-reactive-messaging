package io.smallrye.reactive.messaging.mqtt;

import static org.awaitility.Awaitility.await;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.jboss.logging.Logger;

public class MqttUsage {

    private final static Logger LOGGER = Logger.getLogger(MqttUsage.class);
    private final IMqttClient client;

    public MqttUsage(String host, int port) {
        try {
            client = new MqttClient("tcp://" + host + ":" + port, UUID.randomUUID().toString());
            client.connect();
            await().until(client::isConnected);
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    public MqttUsage(String host, int port, String user, String pwd) {
        try {
            client = new MqttClient("tcp://" + host + ":" + port, UUID.randomUUID().toString());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName(user);
            options.setPassword(pwd.toCharArray());
            client.connect(options);
            await().until(client::isConnected);
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    public MqttUsage(String host, int port, String user, String pwd, Properties tls) {
        try {
            client = new MqttClient("ssl://" + host + ":" + port, UUID.randomUUID().toString());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName(user);
            options.setPassword(pwd.toCharArray());
            options.setSSLProperties(tls);
            client.connect(options);
            await().until(client::isConnected);
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Use the supplied function to asynchronously produce messages and write them to the broker.
     *
     * @param topic the topic, must not be null
     * @param messageCount the number of messages to produce; must be positive
     * @param completionCallback the function to be called when the producer is completed; may be null
     * @param messageSupplier the function to produce messages; may not be null
     */
    public void produce(String topic, int messageCount, Runnable completionCallback, Supplier<byte[]> messageSupplier) {
        Thread t = new Thread(() -> {
            LOGGER.infof("Starting MQTT client to write %s messages", messageCount);
            try {
                for (int i = 0; i != messageCount; ++i) {
                    byte[] payload = messageSupplier.get();
                    client.publish(topic, payload, 0, false);
                }
            } catch (Exception e) {
                LOGGER.error("Unable to send message", e);
            } finally {
                if (completionCallback != null) {
                    completionCallback.run();
                }
            }
        });
        t.setName(topic + "-thread");
        t.start();
    }

    public void produceStrings(String topic, int messageCount, Runnable completionCallback, Supplier<String> messageSupplier) {
        this.produce(topic, messageCount, completionCallback, () -> messageSupplier.get().getBytes());
    }

    public void produceIntegers(String topic, int messageCount, Runnable completionCallback,
            Supplier<Integer> messageSupplier) {
        this.produce(topic, messageCount, completionCallback, () -> messageSupplier.get().toString().getBytes());
    }

    /**
     * Use the supplied function to asynchronously consume messages from the cluster.
     *
     * @param topic the topic
     * @param continuation the function that determines if the consumer should continue; may not be null
     * @param completion the function to call when the consumer terminates; may be null
     * @param messageListener the function to consume the raw messages; may not be null
     */
    public void consumeRaw(String topic, BooleanSupplier continuation, Runnable completion,
            IMqttMessageListener messageListener) {
        CountDownLatch subscribed = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            LOGGER.infof("Starting consumer to read messages on %s", topic);
            try {
                client.subscribe(topic, (top, msg) -> {
                    LOGGER.infof("Consumer %s: consuming message %s", topic, new String(msg.getPayload()));
                    messageListener.messageArrived(top, msg);
                    if (!continuation.getAsBoolean()) {
                        client.unsubscribe(topic);
                    }
                });
                subscribed.countDown();
            } catch (Exception e) {
                LOGGER.errorf("Unable to receive messages from %s", topic, e);
            } finally {
                if (completion != null) {
                    completion.run();
                }
                LOGGER.debugf("Stopping consumer %s", topic);
            }
        });
        t.setName(topic + "-thread");
        t.start();
        try {
            subscribed.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Use the supplied function to asynchronously consume messages from the cluster.
     *
     * @param topic the topic
     * @param continuation the function that determines if the consumer should continue; may not be null
     * @param completion the function to call when the consumer terminates; may be null
     * @param consumerFunction the function to consume the raw messages; may not be null
     */
    public void consume(String topic, BooleanSupplier continuation, Runnable completion,
            java.util.function.Consumer<byte[]> consumerFunction) {
        this.consumeRaw(topic, continuation, completion, (top, msg) -> consumerFunction.accept(msg.getPayload()));
    }

    public void consumeStrings(String topic, BooleanSupplier continuation, Runnable completion,
            Consumer<String> consumerFunction) {
        this.consume(topic, continuation, completion, bytes -> consumerFunction.accept(new String(bytes)));
    }

    public void consumeIntegers(String topic, BooleanSupplier continuation, Runnable completion,
            Consumer<Integer> consumerFunction) {
        this.consume(topic, continuation, completion, bytes -> consumerFunction.accept(Integer.valueOf(new String(bytes))));
    }

    public void consumeStrings(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
            Consumer<String> consumer) {
        AtomicLong readCounter = new AtomicLong();
        this.consumeStrings(topicName, this.continueIfNotExpired(() -> readCounter.get() < (long) count, timeout, unit),
                completion, s -> {
                    consumer.accept(s);
                    readCounter.incrementAndGet();
                });
    }

    public void consumeIntegers(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
            Consumer<Integer> consumer) {
        AtomicLong readCounter = new AtomicLong();
        this.consumeIntegers(topicName, this.continueIfNotExpired(() -> readCounter.get() < (long) count, timeout, unit),
                completion, s -> {
                    consumer.accept(s);
                    readCounter.incrementAndGet();
                });
    }

    public void consumeRaw(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
            IMqttMessageListener messageListener) {
        AtomicLong readCounter = new AtomicLong();
        this.consumeRaw(topicName, this.continueIfNotExpired(() -> readCounter.get() < (long) count, timeout, unit), completion,
                (top, msg) -> {
                    messageListener.messageArrived(top, msg);
                    readCounter.incrementAndGet();
                });
    }

    private BooleanSupplier continueIfNotExpired(BooleanSupplier continuation,
            long timeout, TimeUnit unit) {
        return new BooleanSupplier() {
            long stopTime = 0L;

            public boolean getAsBoolean() {
                if (this.stopTime == 0L) {
                    this.stopTime = System.currentTimeMillis() + unit.toMillis(timeout);
                }

                return continuation.getAsBoolean() && System.currentTimeMillis() <= this.stopTime;
            }
        };
    }

    public void close() {
        try {
            if (client.isConnected()) {
                client.disconnect();
            }
            client.close();
        } catch (MqttException e) {
            LOGGER.error("Unable to close the MQTT client", e);
        } catch (RejectedExecutionException e) {
            // Ignore.
        }
    }
}
