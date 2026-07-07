package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;

public class GracefulShutdownTest extends KafkaCompanionTestBase {

    @Test
    public void testGracefulShutdownDrainsInFlightMessages() {
        String group = "test-graceful-shutdown-drain";

        addBeans(SlowConsumerBean.class);

        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data");
        config.put("group.id", group);
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("commit-strategy", "latest");
        config.put("graceful-shutdown", true);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        runApplication(config);

        SlowConsumerBean bean = get(SlowConsumerBean.class);

        // Wait for some messages to be consumed
        await().atMost(30, TimeUnit.SECONDS).until(() -> bean.getCount() >= 5);

        // Get direct reference before container shutdown
        List<Integer> received = bean.getReceived();
        int countBeforeShutdown = received.size();

        // Close the CDI container — triggers ConfiguredChannelFactory at Priority 40,
        // which drains in-flight messages, calls connector preShutdown/shutdown per channel.
        container.close();
        container = null;

        // After shutdown, the drain should have let in-flight messages complete.
        int countAfterShutdown = received.size();
        assertThat(countAfterShutdown).isGreaterThanOrEqualTo(countBeforeShutdown);

        TopicPartition tp = new TopicPartition(topic, 0);
        await().untilAsserted(() -> {
            OffsetAndMetadata offset = companion.consumerGroups().offsets(group, tp);
            assertThat(offset).isNotNull();
            assertThat(offset.offset()).isEqualTo(countAfterShutdown);
        });
    }

    @Test
    public void testGracefulShutdownDrainsEmitterBufferedMessages() {
        addBeans(EmitterProducerBean.class);

        runApplication(kafkaConfig("mp.messaging.outgoing.out")
                .with("topic", topic)
                .with("key.serializer", StringSerializer.class.getName())
                .with("value.serializer", IntegerSerializer.class.getName())
                .with("graceful-shutdown", true));

        EmitterProducerBean bean = get(EmitterProducerBean.class);
        for (int i = 0; i < 10; i++) {
            bean.send(i);
        }

        // Shut down immediately — graceful shutdown should drain all buffered messages
        container.close();
        container = null;

        List<Integer> received = companion.consumeIntegers().fromTopics(topic, 10)
                .awaitCompletion(Duration.ofSeconds(10))
                .getRecords().stream()
                .map(ConsumerRecord::value)
                .toList();

        assertThat(received).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @ApplicationScoped
    public static class EmitterProducerBean {

        @Inject
        @Channel("out")
        Emitter<Integer> emitter;

        public void send(int value) {
            emitter.send(value);
        }
    }

    @ApplicationScoped
    public static class SlowConsumerBean {

        private final List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        @Blocking
        public void consume(int payload) throws InterruptedException {
            Thread.sleep(100);
            received.add(payload);
        }

        public int getCount() {
            return received.size();
        }

        public List<Integer> getReceived() {
            return received;
        }
    }
}
