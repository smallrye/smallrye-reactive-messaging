package io.smallrye.reactive.messaging.kafka.perf;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

public class PerformanceProducerTest extends KafkaTestBase {

    private static final int COUNT = 100000;

    @Test
    public void testDefault() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        createTopic(topic, 10);
        CountDownLatch receptionDone = new CountDownLatch(1);
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());
        usage.consumeIntegers(topic, COUNT, 1, TimeUnit.MINUTES, receptionDone::countDown, (s, v) -> {
            received.add(v);
        });

        KafkaMapBasedConfig config = KafkaMapBasedConfig.builder("mp.messaging.outgoing.kafka")
                .put("connector", KafkaConnector.CONNECTOR_NAME)
                .put("topic", topic)
                .put("value.serializer", IntegerSerializer.class.getName())
                .build();

        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(1))
                .until(() -> bean.list().size() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        receptionDone.await(1, TimeUnit.MINUTES);

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @Test
    public void testWithoutBackPressure() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        createTopic(topic, 10);
        CountDownLatch receptionDone = new CountDownLatch(1);
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());
        usage.consumeIntegers(topic, COUNT, 1, TimeUnit.MINUTES, receptionDone::countDown, (s, v) -> {
            received.add(v);
        });

        KafkaMapBasedConfig config = KafkaMapBasedConfig.builder("mp.messaging.outgoing.kafka")
                .put("connector", KafkaConnector.CONNECTOR_NAME)
                .put("topic", topic)
                .put("max-inflight-messages", 0L)
                .put("value.serializer", IntegerSerializer.class.getName())
                .build();
        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(1))
                .until(() -> bean.list().size() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        receptionDone.await(1, TimeUnit.MINUTES);

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @Test
    public void testWithoutBackPressureAndNoWait() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        createTopic(topic, 10);
        CountDownLatch receptionDone = new CountDownLatch(1);
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());
        usage.consumeIntegers(topic, COUNT, 1, TimeUnit.MINUTES, receptionDone::countDown, (s, v) -> {
            received.add(v);
        });

        KafkaMapBasedConfig config = KafkaMapBasedConfig.builder("mp.messaging.outgoing.kafka")
                .put("connector", KafkaConnector.CONNECTOR_NAME)
                .put("topic", topic)
                .put("max-inflight-messages", 0L)
                .put("waitForWriteCompletion", false)
                .put("value.serializer", IntegerSerializer.class.getName())
                .build();
        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(1))
                .until(() -> bean.list().size() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        receptionDone.await(1, TimeUnit.MINUTES);

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @Test
    public void testWithoutBackPressureAndIdempotence() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        createTopic(topic, 10);
        CountDownLatch receptionDone = new CountDownLatch(1);
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());
        usage.consumeIntegers(topic, COUNT, 1, TimeUnit.MINUTES, receptionDone::countDown, (s, v) -> {
            received.add(v);
        });

        KafkaMapBasedConfig config = KafkaMapBasedConfig.builder("mp.messaging.outgoing.kafka")
                .put("connector", KafkaConnector.CONNECTOR_NAME)
                .put("topic", topic)
                .put("max-inflight-messages", 0L)
                .put("enable.idempotence", true)
                .put("acks", "all")
                .put("value.serializer", IntegerSerializer.class.getName())
                .build();
        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(1))
                .until(() -> bean.list().size() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        receptionDone.await(1, TimeUnit.MINUTES);

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @Test
    public void testWithoutBackPressureAndIncreaseKafkaRequests() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        createTopic(topic, 10);
        CountDownLatch receptionDone = new CountDownLatch(1);
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());
        usage.consumeIntegers(topic, COUNT, 1, TimeUnit.MINUTES, receptionDone::countDown, (s, v) -> {
            received.add(v);
        });

        KafkaMapBasedConfig config = KafkaMapBasedConfig.builder("mp.messaging.outgoing.kafka")
                .put("connector", KafkaConnector.CONNECTOR_NAME)
                .put("topic", topic)
                .put("max-inflight-messages", 0L)
                .put("max.in.flight.requests.per.connection", 100)
                .put("value.serializer", IntegerSerializer.class.getName())
                .build();
        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(1))
                .until(() -> bean.list().size() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        receptionDone.await(1, TimeUnit.MINUTES);

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @ApplicationScoped
    public static class GeneratorBean {

        private final List<Integer> acked = Collections.synchronizedList(new ArrayList<>());

        @Inject
        @Channel("kafka")
        @OnOverflow(bufferSize = COUNT, value = OnOverflow.Strategy.BUFFER)
        Emitter<Integer> emitter;

        public void run() {
            for (int i = 0; i < COUNT; i++) {
                int v = i;
                Message<Integer> message = Message.of(v, () -> {
                    acked.add(v);
                    return CompletableFuture.completedFuture(null);
                });
                emitter.send(message);
            }
        }

        public List<Integer> list() {
            return acked;
        }

    }

}
