package io.smallrye.reactive.messaging.kafka.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ManualPartitionPauseTest extends KafkaCompanionTestBase {

    private KafkaSource<String, String> source;

    @AfterEach
    void closing() {
        if (source != null) {
            source.closeQuietly();
        }
    }

    @Test
    void testManualPauseWithPauseIfNoRequests() {
        companion.topics().createAndWait(topic, 3);
        String group = UUID.randomUUID().toString();
        source = createSource(group, commonConfiguration());

        List<IncomingKafkaRecord<String, String>> items = new CopyOnWriteArrayList<>();
        source.getStream()
                .onItem().invoke(items::add)
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        TopicPartition tp0 = new TopicPartition(topic, 0);

        companion.produceStrings().usingGenerator(i -> {
            int partition = i % 3;
            return new ProducerRecord<>(topic, partition, "k", "v" + partition + "-" + i);
        }, 15).awaitCompletion(Duration.ofMinutes(1));

        await().until(() -> items.size() >= 15);

        source.getConsumer().pause(Set.of(tp0)).await().indefinitely();
        assertThat(source.getConsumer().manuallyPaused()).containsExactly(tp0);

        companion.produceStrings().usingGenerator(i -> {
            int partition = i % 3;
            return new ProducerRecord<>(topic, partition, "k", "v" + partition + "-" + (15 + i));
        }, 15).awaitCompletion(Duration.ofMinutes(1));

        await().until(() -> items.stream().filter(r -> r.getPartition() != 0).count() >= 15);

        List<IncomingKafkaRecord<String, String>> partition0Records = items.stream()
                .filter(r -> r.getPartition() == 0)
                .collect(Collectors.toList());
        assertThat(partition0Records).hasSize(5);
    }

    @Test
    void testManualResumeOfPausedPartition() {
        companion.topics().createAndWait(topic, 2);
        String group = UUID.randomUUID().toString();
        source = createSource(group, commonConfiguration());

        List<IncomingKafkaRecord<String, String>> items = new CopyOnWriteArrayList<>();
        source.getStream()
                .onItem().invoke(items::add)
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        TopicPartition tp0 = new TopicPartition(topic, 0);

        companion.produceStrings().usingGenerator(i -> {
            int partition = i % 2;
            return new ProducerRecord<>(topic, partition, "k", "v" + partition + "-" + i);
        }, 10).awaitCompletion(Duration.ofMinutes(1));

        await().until(() -> items.size() >= 10);

        source.getConsumer().pause(Set.of(tp0)).await().indefinitely();
        assertThat(source.getConsumer().manuallyPaused()).containsExactly(tp0);

        source.getConsumer().resume(Set.of(tp0)).await().indefinitely();
        assertThat(source.getConsumer().manuallyPaused()).isEmpty();

        companion.produceStrings().usingGenerator(i -> {
            int partition = i % 2;
            return new ProducerRecord<>(topic, partition, "k", "v" + partition + "-" + (10 + i));
        }, 10).awaitCompletion(Duration.ofMinutes(1));

        await().until(() -> items.stream().filter(r -> r.getPartition() == 0).count() >= 10);
    }

    @Test
    void testBackpressureResumeDoesNotResumeManuallyPaused() {
        companion.topics().createAndWait(topic, 2);
        String group = UUID.randomUUID().toString();
        MapBasedConfig config = commonConfiguration()
                .with(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3);
        source = createSource(group, config);

        AssertSubscriber<IncomingKafkaRecord<String, String>> subscriber = source.getStream()
                .subscribe().withSubscriber(AssertSubscriber.create(1));

        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);

        companion.produceStrings().fromRecords(
                new ProducerRecord<>(topic, 0, "k", "v0")).awaitCompletion(Duration.ofMinutes(1));

        await().until(() -> subscriber.getItems().size() >= 1);

        source.getConsumer().pause(Set.of(tp1)).await().indefinitely();

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, 0, "k", "v0-" + i), 20)
                .awaitCompletion(Duration.ofMinutes(1));

        await().until(() -> source.getConsumer().paused()
                .await().indefinitely().contains(tp0));

        subscriber.request(100);

        await().until(() -> !source.getConsumer().paused()
                .await().indefinitely().contains(tp0));

        assertThat(source.getConsumer().paused().await().indefinitely()).contains(tp1);
        assertThat(source.getConsumer().manuallyPaused()).containsExactly(tp1);
    }

    @Test
    void testGlobalPauseResumeWithManuallyPausedPartition() {
        companion.topics().createAndWait(topic, 2);
        String group = UUID.randomUUID().toString();
        MapBasedConfig config = commonConfiguration()
                .with("pause-if-no-requests", false);
        source = createSource(group, config);

        List<IncomingKafkaRecord<String, String>> items = new CopyOnWriteArrayList<>();
        source.getStream()
                .onItem().invoke(items::add)
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);

        await().until(() -> source.getConsumer().getAssignments()
                .await().indefinitely().size() >= 2);

        source.getConsumer().pause(Set.of(tp0)).await().indefinitely();

        source.getConsumer().pause().await().indefinitely();
        assertThat(source.getConsumer().paused().await().indefinitely())
                .containsExactlyInAnyOrder(tp0, tp1);

        source.getConsumer().resume().await().indefinitely();

        await().untilAsserted(() -> assertThat(source.getConsumer().paused().await().indefinitely())
                .containsExactly(tp0));

        assertThat(source.getConsumer().manuallyPaused()).containsExactly(tp0);
    }

    private MapBasedConfig commonConfiguration() {
        return kafkaConfig()
                .build("channel-name", topic,
                        "graceful-shutdown", false,
                        "topic", topic,
                        "health-enabled", false,
                        "auto.offset.reset", "earliest",
                        "value.deserializer", StringDeserializer.class.getName());
    }
}
