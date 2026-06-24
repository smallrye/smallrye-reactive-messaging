package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
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

        // Close the CDI container — triggers GracefulShutdownController
        // at Priority 40 (pauseAndDrain) before KafkaConnector at Priority 50.
        // The drain ensures in-flight @Blocking messages are acked before
        // the connector shuts down.
        container.close();
        container = null;

        // After shutdown, the drain should have let in-flight messages complete.
        // The committed offset should match the number of processed messages:
        // - GracefulShutdownController drained in-flight acks (WIP=0)
        // - handle() Uni resolves only after offsets.put() (emitter-based)
        // - terminate() does a final commitSync on the Vert.x context (FIFO after handle lambdas)
        int countAfterShutdown = received.size();
        assertThat(countAfterShutdown).isGreaterThanOrEqualTo(countBeforeShutdown);

        TopicPartition tp = new TopicPartition(topic, 0);
        await().untilAsserted(() -> {
            OffsetAndMetadata offset = companion.consumerGroups().offsets(group, tp);
            assertThat(offset).isNotNull();
            assertThat(offset.offset()).isEqualTo(countAfterShutdown);
        });
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
