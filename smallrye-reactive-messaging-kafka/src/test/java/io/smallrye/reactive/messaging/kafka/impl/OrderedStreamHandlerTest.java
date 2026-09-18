package io.smallrye.reactive.messaging.kafka.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;

public class OrderedStreamHandlerTest extends KafkaCompanionTestBase {

    @Test
    public void testOrderedGroupsResetOnRebalance() {
        companion.topics().createAndWait(topic, 2);

        int initialBatch = 10;
        companion.produceStrings()
                .fromRecords(IntStream.range(0, initialBatch).boxed()
                        .flatMap(i -> IntStream.range(0, 2).boxed()
                                .map(p -> new ProducerRecord<>(topic, p, "key-" + (i % 3), "value-p" + p + "-" + i)))
                        .toList())
                .awaitCompletion();

        String groupId = "test-rebalance-ordered-" + UUID.randomUUID();

        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data")
                .put("topic", topic)
                .put("group.id", groupId)
                .put("auto.offset.reset", "earliest")
                .put("value.deserializer", StringDeserializer.class.getName())
                .put("key.deserializer", StringDeserializer.class.getName())
                .put("ordered", "key")
                .put("commit-strategy", "throttled")
                .put("auto.commit.interval.ms", 100);

        addBeans(OrderedConsumerBean.class);
        runApplication(config);

        OrderedConsumerBean bean = get(OrderedConsumerBean.class);

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(bean.received()).hasSize(initialBatch * 2));

        // Second consumer joins — triggers rebalance, steals one partition
        var secondConsumer = companion.consumeStrings()
                .withGroupId(groupId)
                .fromTopics(topic);

        await().pollDelay(2, TimeUnit.SECONDS).until(() -> true);

        int countAfterRebalance = bean.received().size();

        // Produce more messages
        companion.produceStrings()
                .fromRecords(IntStream.range(initialBatch, initialBatch + 10).boxed()
                        .flatMap(i -> IntStream.range(0, 2).boxed()
                                .map(p -> new ProducerRecord<>(topic, p, "key-" + (i % 3), "value-p" + p + "-" + i)))
                        .toList())
                .awaitCompletion();

        // Our consumer should still process messages from its assigned partition
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> bean.received().size() > countAfterRebalance);

        // Close second consumer — partition re-assigned back
        secondConsumer.close();

        await().pollDelay(2, TimeUnit.SECONDS).until(() -> true);

        // Produce a final batch — both partitions should be consumed again
        companion.produceStrings()
                .fromRecords(IntStream.range(20, 30).boxed()
                        .flatMap(i -> IntStream.range(0, 2).boxed()
                                .map(p -> new ProducerRecord<>(topic, p, "key-" + (i % 3), "value-p" + p + "-" + i)))
                        .toList())
                .awaitCompletion();

        // Verify messages from both partitions are received after re-assignment
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> bean.received().stream().anyMatch(s -> s.contains("value-p0-2"))
                        && bean.received().stream().anyMatch(s -> s.contains("value-p1-2")));
    }

    @Test
    public void testOrderedGroupsResetOnSeek() {
        String group = "test-seek-ordered";

        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data")
                .put("group.id", group)
                .put("topic", topic)
                .put("value.deserializer", StringDeserializer.class.getName())
                .put("key.deserializer", StringDeserializer.class.getName())
                .put("enable.auto.commit", "false")
                .put("auto.offset.reset", "earliest")
                .put("commit-strategy", "throttled")
                .put("auto.commit.interval.ms", 100)
                .put("ordered", "key")
                .put("pausable", true);

        addBeans(OrderedSeekConsumerBean.class);
        runApplication(config);

        OrderedSeekConsumerBean bean = get(OrderedSeekConsumerBean.class);

        TopicPartition tp = new TopicPartition(topic, 0);

        companion.produceStrings()
                .usingGenerator(i -> new ProducerRecord<>(topic, 0, "key-" + (i % 3), "value-" + i), 10);

        await().atMost(30, TimeUnit.SECONDS).until(() -> bean.getCount() >= 10);

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    OffsetAndMetadata offset = companion.consumerGroups().offsets(group, tp);
                    assertNotNull(offset);
                    assertEquals(10L, offset.offset());
                });

        bean.seekToBeginning(Collections.singleton(tp));

        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> bean.getCount() >= 20);

        for (int i = 0; i < 3; i++) {
            assertThat(bean.keyMaxConcurrency("key-" + i)).hasValue(1);
        }
    }

    @ApplicationScoped
    public static class OrderedConsumerBean {
        private final List<String> received = new CopyOnWriteArrayList<>();
        private final Map<String, List<String>> receivedByKey = new ConcurrentHashMap<>();

        @Incoming("data")
        @Blocking(ordered = false)
        public void consume(String payload, IncomingKafkaRecordMetadata<String, String> metadata)
                throws InterruptedException {
            received.add(payload);
            receivedByKey.computeIfAbsent(metadata.getKey(), k -> new CopyOnWriteArrayList<>()).add(payload);
            Thread.sleep(50);
        }

        public List<String> received() {
            return received;
        }

        public Map<String, List<String>> receivedByKey() {
            return receivedByKey;
        }
    }

    @ApplicationScoped
    public static class OrderedSeekConsumerBean {

        @Inject
        @Channel("data")
        PausableChannel pausable;

        @Inject
        KafkaClientService clientService;

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger wip = new AtomicInteger();
        private final Map<String, AtomicInteger> keyCounter = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> keyMaxCounter = new ConcurrentHashMap<>();

        @Incoming("data")
        @Blocking(ordered = false)
        public void consume(String payload, IncomingKafkaRecordMetadata<String, String> metadata) {
            wip.incrementAndGet();
            try {
                received.add(payload);
                String key = metadata.getKey();
                AtomicInteger counter = keyCounter.computeIfAbsent(key, k -> new AtomicInteger(0));
                int current = counter.incrementAndGet();
                keyMaxCounter.computeIfAbsent(key, k -> new AtomicInteger(0))
                        .updateAndGet(max -> Math.max(max, current));
                counter.decrementAndGet();
            } finally {
                wip.decrementAndGet();
            }
        }

        public void seekToBeginning(Collection<TopicPartition> partitions) {
            pausable.pause();
            await().until(() -> wip.get() == 0);
            clientService.<String, String> getConsumer("data")
                    .seekToBeginning(partitions).await().indefinitely();
            pausable.clearBuffer();
            pausable.resume();
        }

        public int getCount() {
            return received.size();
        }

        public AtomicInteger keyMaxConcurrency(String key) {
            return keyMaxCounter.get(key);
        }
    }
}
