package io.smallrye.reactive.messaging.kafka;

import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.RepeatedTest;

import io.smallrye.reactive.messaging.kafka.base.KafkaBrokerExtension;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class SourceCloseTest extends KafkaTestBase {

    @RepeatedTest(5)
    public void testNoLostMessagesOnClose() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger count = new AtomicInteger();
        int messageCount = 1000;
        usage.produceIntegers(messageCount, latch::countDown, () -> new ProducerRecord<>(topic, null, count.getAndIncrement()));
        latch.await();

        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config1 = new MapBasedConfig()
                .with("channel-name", "data1")
                .with("bootstrap.servers", KafkaBrokerExtension.getBootstrapServers())
                .with("topic", topic)
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("max.poll.records", 4)
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .with(ConsumerConfig.CLIENT_ID_CONFIG, "A");

        MapBasedConfig config2 = new MapBasedConfig()
                .with("channel-name", "data2")
                .with("bootstrap.servers", KafkaBrokerExtension.getBootstrapServers())
                .with("topic", topic)
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("max.poll.records", 4)
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .with(ConsumerConfig.CLIENT_ID_CONFIG, "B");

        List<Integer> list = new ArrayList<>();

        KafkaSource<String, Integer> source1 = new KafkaSource<>(vertx, groupId,
                new KafkaConnectorIncomingConfiguration(config1), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 0);
        KafkaSource<String, Integer> source2 = new KafkaSource<>(vertx, groupId,
                new KafkaConnectorIncomingConfiguration(config2), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 0);

        source1.getStream()
                .subscribe().with(l -> {
                    list.add(l.getPayload());
                    CompletableFuture.runAsync(l::ack);
                });

        source2.getStream()
                .subscribe().with(l -> {
                    list.add(l.getPayload());
                    CompletableFuture.runAsync(l::ack);
                });

        await().until(() -> list.size() >= 100);
        source1.closeQuietly();

        await().until(() -> list.size() == 1000);

        source2.closeQuietly();
    }

}
