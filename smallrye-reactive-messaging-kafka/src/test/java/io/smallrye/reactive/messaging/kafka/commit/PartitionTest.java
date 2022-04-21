package io.smallrye.reactive.messaging.kafka.commit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PartitionTest extends KafkaCompanionTestBase {

    @Test
    public void testWithPartitions() {
        companion.topics().createAndWait(topic, 3);
        String groupId = UUID.randomUUID().toString();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName());

        MyApplication application = runApplication(config, MyApplication.class);

        int expected = 3000;
        Random random = new Random();
        companion.produceStrings().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), Integer.toString(i));
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> application.count() == expected);
        assertThat(application.getReceived().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));

        Properties properties = new Properties();
        properties.put("bootstrap.servers", companion.getBootstrapServers());
        Admin admin = Admin.create(properties);

        await().until(() -> {
            Map<TopicPartition, OffsetAndMetadata> map = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get();
            long c = map.values().stream().map(OffsetAndMetadata::offset).mapToLong(l -> l).sum();
            return map.size() == 3 && c == expected;
        });
    }

    @Test
    public void testWithMoreConsumersThanPartitions() throws InterruptedException {
        companion.topics().createAndWait(topic, 3);
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 5) // 2 idles
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName());

        MyApplication application = runApplication(config, MyApplication.class);

        int expected = 3000;
        Random random = new Random();
        companion.produceStrings().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), Integer.toString(i));
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> application.count() == expected);
        assertThat(application.getReceived().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));

        Properties properties = new Properties();
        properties.put("bootstrap.servers", companion.getBootstrapServers());
        Admin admin = Admin.create(properties);

        await().until(() -> {
            Map<TopicPartition, OffsetAndMetadata> map = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get();
            long c = map.values().stream().map(OffsetAndMetadata::offset).mapToLong(l -> l).sum();
            return map.size() == 3 && c == expected;
        });
    }

    @Test
    public void testWithMorePartitionsThanConsumers() throws InterruptedException {
        companion.topics().createAndWait(topic, 3);
        String groupId = UUID.randomUUID().toString();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 2) // one consumer will get 2 partitions
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName());

        MyApplication application = runApplication(config, MyApplication.class);

        int expected = 3000;
        Random random = new Random();
        companion.produceStrings().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), Integer.toString(i));
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> application.count() == expected);
        assertThat(application.getReceived().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(2));

        Properties properties = new Properties();
        properties.put("bootstrap.servers", companion.getBootstrapServers());
        Admin admin = Admin.create(properties);

        await().until(() -> {
            Map<TopicPartition, OffsetAndMetadata> map = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get();
            long c = map.values().stream().map(OffsetAndMetadata::offset).mapToLong(l -> l).sum();
            return map.size() == 3 && c == expected;
        });
    }

    @ApplicationScoped
    public static class MyApplication {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<String>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        public void consume(String payload) {
            String k = Thread.currentThread().getName();
            List<String> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
            list.add(payload);
            count.incrementAndGet();
        }

        public Map<String, List<String>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

    private int getMaxNumberOfEventLoop(int expected) {
        // On Github Actions, only one event loop is created.
        return Math.min(expected, Runtime.getRuntime().availableProcessors() / 2);
    }

}
