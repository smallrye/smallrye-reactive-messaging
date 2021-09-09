package io.smallrye.reactive.messaging.kafka.commit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PartitionTest extends KafkaTestBase {

    @Test
    public void testWithPartitions() throws InterruptedException {
        createTopic(topic, 3);
        String groupId = UUID.randomUUID().toString();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.kafka.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.kafka.group.id", groupId)
                .with("mp.messaging.incoming.kafka.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.kafka.topic", topic)
                .with("mp.messaging.incoming.kafka.partitions", 3)
                .with("mp.messaging.incoming.kafka.auto.offset.reset", "earliest")
                .with("mp.messaging.incoming.kafka.value.deserializer", StringDeserializer.class.getName());

        MyApplication application = runApplication(config, MyApplication.class);

        AtomicInteger count = new AtomicInteger();
        int expected = 3000;
        Random random = new Random();
        CountDownLatch latch = new CountDownLatch(1);
        usage.produce(topic, expected, new StringSerializer(), new StringSerializer(), latch::countDown, () -> {
            int value = count.getAndIncrement();
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), Integer.toString(value));
        });

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();

        await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> application.count() == expected);
        assertThat(application.getReceived().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));

        Properties properties = new Properties();
        properties.put("bootstrap.servers", getBootstrapServers());
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
        createTopic(topic, 3);
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.kafka.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.kafka.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.kafka.group.id", groupId)
                .with("mp.messaging.incoming.kafka.topic", topic)
                .with("mp.messaging.incoming.kafka.partitions", 5) // 2 idles
                .with("mp.messaging.incoming.kafka.auto.offset.reset", "earliest")
                .with("mp.messaging.incoming.kafka.value.deserializer", StringDeserializer.class.getName());

        MyApplication application = runApplication(config, MyApplication.class);

        AtomicInteger count = new AtomicInteger();
        int expected = 3000;
        Random random = new Random();
        CountDownLatch latch = new CountDownLatch(1);
        usage.produce(topic, expected, new StringSerializer(), new StringSerializer(), latch::countDown, () -> {
            int value = count.getAndIncrement();
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), Integer.toString(value));
        });

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();

        await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> application.count() == expected);
        assertThat(application.getReceived().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));

        Properties properties = new Properties();
        properties.put("bootstrap.servers", getBootstrapServers());
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
        createTopic(topic, 3);
        String groupId = UUID.randomUUID().toString();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.kafka.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.kafka.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.kafka.group.id", groupId)
                .with("mp.messaging.incoming.kafka.topic", topic)
                .with("mp.messaging.incoming.kafka.partitions", 2) // one consumer will get 2 partitions
                .with("mp.messaging.incoming.kafka.auto.offset.reset", "earliest")
                .with("mp.messaging.incoming.kafka.value.deserializer", StringDeserializer.class.getName());

        MyApplication application = runApplication(config, MyApplication.class);

        AtomicInteger count = new AtomicInteger();
        int expected = 3000;
        Random random = new Random();
        CountDownLatch latch = new CountDownLatch(1);
        usage.produce(topic, expected, new StringSerializer(), new StringSerializer(), latch::countDown, () -> {
            int value = count.getAndIncrement();
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), Integer.toString(value));
        });

        assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();

        await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> application.count() == expected);
        assertThat(application.getReceived().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(2));

        Properties properties = new Properties();
        properties.put("bootstrap.servers", getBootstrapServers());
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
