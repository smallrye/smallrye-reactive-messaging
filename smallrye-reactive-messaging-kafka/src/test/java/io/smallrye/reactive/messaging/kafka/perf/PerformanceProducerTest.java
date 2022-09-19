package io.smallrye.reactive.messaging.kafka.perf;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;

public class PerformanceProducerTest extends KafkaCompanionTestBase {

    private static final int COUNT = 100_000;
    private static final int TIMEOUT_IN_MINUTES = 1;

    @Test
    public void testDefault() {
        String topic = UUID.randomUUID().toString();
        companion.topics().createAndWait(topic, 10);
        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(topic, COUNT, Duration.ofMinutes(1));

        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.outgoing.kafka")
                .put("topic", topic)
                .put("value.serializer", IntegerSerializer.class.getName());

        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(TIMEOUT_IN_MINUTES))
                .until(() -> bean.count() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        records.awaitCompletion(Duration.ofMinutes(TIMEOUT_IN_MINUTES));

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @Test
    public void testWithoutBackPressure() {
        String topic = UUID.randomUUID().toString();
        companion.topics().createAndWait(topic, 10);
        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(topic, COUNT,
                Duration.ofMinutes(TIMEOUT_IN_MINUTES));

        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.outgoing.kafka")
                .put("topic", topic)
                .put("max-inflight-messages", 0L)
                .put("value.serializer", IntegerSerializer.class.getName());

        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(TIMEOUT_IN_MINUTES))
                .until(() -> bean.count() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        records.awaitCompletion(Duration.ofMinutes(TIMEOUT_IN_MINUTES));

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @Test
    public void testWithoutBackPressureAndNoWait() {
        String topic = UUID.randomUUID().toString();
        companion.topics().createAndWait(topic, 10);
        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(topic, COUNT,
                Duration.ofMinutes(TIMEOUT_IN_MINUTES));

        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.outgoing.kafka")
                .put("topic", topic)
                .put("max-inflight-messages", 0L)
                .put("waitForWriteCompletion", false)
                .put("value.serializer", IntegerSerializer.class.getName());

        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(TIMEOUT_IN_MINUTES))
                .until(() -> bean.count() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        consumed.awaitCompletion(Duration.ofMinutes(TIMEOUT_IN_MINUTES));

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @Test
    public void testWithoutBackPressureAndIdempotence() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        companion.topics().createAndWait(topic, 10);
        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(topic, COUNT,
                Duration.ofMinutes(TIMEOUT_IN_MINUTES));

        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.outgoing.kafka")
                .put("topic", topic)
                .put("max-inflight-messages", 0L)
                .put("enable.idempotence", true)
                .put("acks", "all")
                .put("value.serializer", IntegerSerializer.class.getName());

        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(TIMEOUT_IN_MINUTES))
                .until(() -> bean.count() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        consumed.awaitCompletion(Duration.ofMinutes(TIMEOUT_IN_MINUTES));

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @Test
    public void testWithoutBackPressureAndIncreaseKafkaRequests() {
        String topic = UUID.randomUUID().toString();
        companion.topics().createAndWait(topic, 10);
        ConsumerTask<String, Integer> consumed = companion.consumeIntegers().fromTopics(topic, COUNT,
                Duration.ofMinutes(TIMEOUT_IN_MINUTES));

        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.outgoing.kafka")
                .put("topic", topic)
                .put("max-inflight-messages", 0L)
                .put("max.in.flight.requests.per.connection", 100)
                .put("value.serializer", IntegerSerializer.class.getName());

        GeneratorBean bean = runApplication(config, GeneratorBean.class);
        await().until(this::isReady);
        await().until(this::isAlive);
        long begin = System.currentTimeMillis();
        bean.run();
        await()
                .atMost(Duration.ofMinutes(TIMEOUT_IN_MINUTES))
                .until(() -> bean.count() == COUNT);
        long end = System.currentTimeMillis();

        // Wait until all the messages are read.
        consumed.awaitCompletion(Duration.ofMinutes(TIMEOUT_IN_MINUTES));

        long duration = end - begin;
        System.out.println("Time " + duration + " ms");
        double speed = (COUNT * 1.0) / (duration / 1000.0);
        System.out.println(speed + " messages/ms");
    }

    @ApplicationScoped
    public static class GeneratorBean {

        private final LongAdder counter = new LongAdder();

        @Inject
        @Channel("kafka")
        @OnOverflow(bufferSize = COUNT, value = OnOverflow.Strategy.BUFFER)
        Emitter<Integer> emitter;

        public void run() {
            for (int i = 0; i < COUNT; i++) {
                int v = i;
                Message<Integer> message = Message.of(v, () -> {
                    counter.increment();
                    return CompletableFuture.completedFuture(null);
                });
                emitter.send(message);
            }
        }

        public long count() {
            return counter.sum();
        }

    }

}
