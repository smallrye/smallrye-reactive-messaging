package io.smallrye.reactive.messaging.kafka.commit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;

public class ConsumerSeekTest extends KafkaCompanionTestBase {

    private KafkaMapBasedConfig commonConfig(String group, String commitStrategy) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data");
        config.put("group.id", group);
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("commit-strategy", commitStrategy);
        config.put("pausable", true);
        config.put("consumer-rebalance-listener.name", "seek-listener");
        if ("throttled".equals(commitStrategy)) {
            config.put("auto.commit.interval.ms", 100);
        }
        return config;
    }

    @Test
    public void testSeekToBeginningWithLatestStrategy() {
        String group = "test-seek-pausable-latest";

        addBeans(SeekConsumerBean.class, SeekRebalanceListener.class);
        runApplication(commonConfig(group, "latest"));

        SeekConsumerBean bean = get(SeekConsumerBean.class);
        SeekRebalanceListener listener = getBeanManager().createInstance()
                .select(SeekRebalanceListener.class)
                .select(Identifier.Literal.of("seek-listener"))
                .get();

        TopicPartition tp = new TopicPartition(topic, 0);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(30, TimeUnit.SECONDS).until(() -> bean.getCount() >= 10);

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    OffsetAndMetadata offset = companion.consumerGroups().offsets(group, tp);
                    assertNotNull(offset);
                    assertEquals(10L, offset.offset());
                });

        bean.seekToBeginning(Collections.singleton(tp));

        assertThat(listener.getSeekedPartitions()).contains(tp);

        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> bean.getCount() >= 20);

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    OffsetAndMetadata offset = companion.consumerGroups().offsets(group, tp);
                    assertNotNull(offset);
                    assertEquals(10L, offset.offset());
                });

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, 10 + i), 10);

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    OffsetAndMetadata offset = companion.consumerGroups().offsets(group, tp);
                    assertNotNull(offset);
                    assertEquals(20L, offset.offset());
                });
    }

    @Test
    public void testSeekWithThrottledStrategy() {
        String group = "test-seek-pausable-throttled";

        addBeans(SeekConsumerBean.class, SeekRebalanceListener.class);
        runApplication(commonConfig(group, "throttled"));

        SeekConsumerBean bean = get(SeekConsumerBean.class);
        SeekRebalanceListener listener = getBeanManager().createInstance()
                .select(SeekRebalanceListener.class)
                .select(Identifier.Literal.of("seek-listener"))
                .get();

        TopicPartition tp = new TopicPartition(topic, 0);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(30, TimeUnit.SECONDS).until(() -> bean.getCount() >= 10);

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    OffsetAndMetadata offset = companion.consumerGroups().offsets(group, tp);
                    assertNotNull(offset);
                    assertEquals(10L, offset.offset());
                });

        bean.seekToBeginning(Collections.singleton(tp));

        assertThat(listener.getSeekedPartitions()).contains(tp);

        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> bean.getCount() >= 20);

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    OffsetAndMetadata offset = companion.consumerGroups().offsets(group, tp);
                    assertNotNull(offset);
                    assertEquals(10L, offset.offset());
                });

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, 10 + i), 10);

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    OffsetAndMetadata offset = companion.consumerGroups().offsets(group, tp);
                    assertNotNull(offset);
                    assertEquals(20L, offset.offset());
                });
    }

    @Test
    public void testSeekToBeginningWithOffsetResetInListener() {
        String group = "test-seek-with-offset-reset";

        addBeans(SeekConsumerBean.class, SeekAndResetOffsetListener.class);

        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data");
        config.put("group.id", group);
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("commit-strategy", "latest");
        config.put("pausable", true);
        config.put("consumer-rebalance-listener.name", "seek-and-reset-listener");

        runApplication(config);

        SeekConsumerBean bean = get(SeekConsumerBean.class);

        TopicPartition tp = new TopicPartition(topic, 0);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(30, TimeUnit.SECONDS).until(() -> bean.getCount() >= 10);

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    OffsetAndMetadata offset = companion.consumerGroups().offsets(group, tp);
                    assertNotNull(offset);
                    assertEquals(10L, offset.offset());
                });

        int countBeforeSeek = bean.getCount();

        // The listener resets the committed offset to the seeked position
        // in onPartitionsSeeked, so replayed records MUST be re-committed.
        // Without partitionsSeeked clearing the commit handler state,
        // the stale offset (10) would block all commits for records 0-9,
        // and the committed offset would stay at 0.
        bean.seekToBeginning(Collections.singleton(tp));

        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> bean.getCount() >= countBeforeSeek + 10);

        // Committed offset must advance back to 10
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    OffsetAndMetadata offset = companion.consumerGroups().offsets(group, tp);
                    assertNotNull(offset);
                    assertEquals(10L, offset.offset());
                });
    }

    @ApplicationScoped
    public static class SeekConsumerBean {

        @Inject
        @Channel("data")
        PausableChannel pausable;

        @Inject
        KafkaClientService clientService;

        private final List<Integer> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger wip = new AtomicInteger();

        @Incoming("data")
        @Blocking
        public void consume(int payload) {
            wip.incrementAndGet();
            try {
                received.add(payload);
            } finally {
                wip.decrementAndGet();
            }
        }

        public void seekToBeginning(Collection<TopicPartition> partitions) {
            pausable.pause();
            await().until(() -> wip.get() == 0);
            clientService.<String, Integer> getConsumer("data")
                    .seekToBeginning(partitions).await().indefinitely();
            pausable.clearBuffer();
            pausable.resume();
        }

        public int getCount() {
            return received.size();
        }

        public List<Integer> getReceived() {
            return received;
        }
    }

    @ApplicationScoped
    @Identifier("seek-listener")
    public static class SeekRebalanceListener implements KafkaConsumerRebalanceListener {

        private final Set<TopicPartition> seekedPartitions = Collections.newSetFromMap(new ConcurrentHashMap<>());

        @Override
        public void onPartitionsSeeked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            seekedPartitions.addAll(partitions);
        }

        public Set<TopicPartition> getSeekedPartitions() {
            return seekedPartitions;
        }
    }

    @ApplicationScoped
    @Identifier("seek-and-reset-listener")
    public static class SeekAndResetOffsetListener implements KafkaConsumerRebalanceListener {

        @Override
        public void onPartitionsSeeked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for (TopicPartition tp : partitions) {
                offsets.put(tp, new OffsetAndMetadata(consumer.position(tp)));
            }
            consumer.commitSync(offsets);
        }
    }
}
