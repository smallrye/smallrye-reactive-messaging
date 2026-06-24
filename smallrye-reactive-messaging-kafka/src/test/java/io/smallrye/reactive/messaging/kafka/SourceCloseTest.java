package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class SourceCloseTest extends KafkaCompanionTestBase {

    @Test
    public void testNoLostMessagesOnClose() {
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, null, i), 1000)
                .awaitCompletion();

        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config1 = new MapBasedConfig()
                .with("channel-name", "data1")
                .with("bootstrap.servers", companion.getBootstrapServers())
                .with("topic", topic)
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("max.poll.records", 4)
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .with(ConsumerConfig.CLIENT_ID_CONFIG, "A");

        MapBasedConfig config2 = new MapBasedConfig()
                .with("channel-name", "data2")
                .with("bootstrap.servers", companion.getBootstrapServers())
                .with("topic", topic)
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("max.poll.records", 4)
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .with(ConsumerConfig.CLIENT_ID_CONFIG, "B");

        List<Integer> list = new CopyOnWriteArrayList<>();

        KafkaSource<String, Integer> source1 = createSource(groupId, config1);
        KafkaSource<String, Integer> source2 = createSource(groupId, config2);

        source1.getStream()
                .invoke(l -> list.add(l.getPayload()))
                .call(l -> Uni.createFrom().completionStage(l::ack))
                .subscribe().with(l -> {
                });

        source2.getStream()
                .invoke(l -> list.add(l.getPayload()))
                .call(l -> Uni.createFrom().completionStage(l::ack))
                .subscribe().with(l -> {
                });

        await().untilAsserted(() -> assertThat(list).hasSizeGreaterThanOrEqualTo(100));
        source1.closeQuietly();

        await().untilAsserted(() -> assertThat(list).hasSizeGreaterThanOrEqualTo(1000));

        source2.closeQuietly();
    }

}
