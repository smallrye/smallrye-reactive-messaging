package io.smallrye.reactive.messaging.kafka.client;

import static io.smallrye.reactive.messaging.kafka.companion.test.KafkaBrokerExtension.restart;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.TestTags;
import io.smallrye.reactive.messaging.kafka.companion.test.KafkaBrokerExtension;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaConsumer;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.strimzi.test.container.StrimziKafkaContainer;

@Tag(TestTags.SLOW)
// TODO should not extend ClientTestBase, it uses KafkaBrokerExtension which creates a broker for tests
public class BrokerRestartTest extends ClientTestBase {

    @Test
    public void testAcknowledgementUsingThrottledStrategyEvenAfterBrokerRestart() throws Exception {
        try (StrimziKafkaContainer kafka = KafkaBrokerExtension.createKafkaContainer()) {
            kafka.start();
            await().until(kafka::isRunning);

            String groupId = UUID.randomUUID().toString();
            MapBasedConfig config = createConsumerConfig(groupId)
                    .with("topic", topic)
                    .with("auto.commit.interval.ms", 10);

            Multi<IncomingKafkaRecord<Integer, String>> stream = createSource(config, groupId).getStream()
                    .invoke(IncomingKafkaRecord::ack);

            CountDownLatch latch = new CountDownLatch(100);
            subscribe(stream, latch);
            try (final StrimziKafkaContainer ignored = restart(kafka, 3)) {
                sendMessages(0, 100);
                waitForMessages(latch);
                checkConsumedMessages(0, 100);
                waitForCommits(source, 100);
            }
        }
    }

    @Test
    public void testResumingPausingWhileBrokerIsDown() throws Exception {
        try (StrimziKafkaContainer kafka = KafkaBrokerExtension.createKafkaContainer()) {
            kafka.start();
            await().until(kafka::isRunning);
            String groupId = UUID.randomUUID().toString();
            MapBasedConfig config = createConsumerConfig(groupId)
                    .with("topic", topic)
                    .with(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
                    .with(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);
            createSource(config, groupId);
            source
                    .getStream().subscribe().withSubscriber(AssertSubscriber.create(1));

            waitForPartitionAssignment();

            ReactiveKafkaConsumer<Integer, String> consumer = source.getConsumer();

            await().until(() -> !consumer.getAssignments().await().indefinitely().isEmpty());
            assertThat(consumer.pause().await().indefinitely()).isNotEmpty();

            consumer.resume().await().indefinitely();
            assertThat(consumer.paused().await().indefinitely()).isEmpty();

            kafka.stop();
            await().until(() -> !kafka.isRunning());

            assertThat(consumer.pause().await().indefinitely()).isNotEmpty();
            consumer.resume().await().indefinitely();
            assertThat(consumer.paused().await().indefinitely()).isEmpty();
        }
    }

    @Test
    public void testPausingWhileBrokerIsDown() throws Exception {
        try (StrimziKafkaContainer kafka = KafkaBrokerExtension.createKafkaContainer()) {
            kafka.start();
            await().until(kafka::isRunning);
            Integer port = kafka.getMappedPort(KAFKA_PORT);
            sendMessages(0, 10, kafka.getBootstrapServers());
            String groupId = UUID.randomUUID().toString();
            MapBasedConfig config = createConsumerConfig(groupId)
                    .with("topic", topic)
                    .with(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
                    .with(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);
            createSource(config, groupId);
            Multi<IncomingKafkaRecord<Integer, String>> stream = source.getStream();

            AssertSubscriber<IncomingKafkaRecord<Integer, String>> subscriber = stream
                    .onItem().invoke(item -> CompletableFuture.runAsync(item::ack))
                    .subscribe().withSubscriber(AssertSubscriber.create(0));

            waitForPartitionAssignment();

            await().untilAsserted(() -> subscriber.assertSubscribed().assertHasNotReceivedAnyItem());

            subscriber.request(1);
            await().until(() -> subscriber.getItems().size() == 1);
            await().until(() -> !source.getConsumer().paused().await().indefinitely().isEmpty());

            sendMessages(0, 10, kafka.getBootstrapServers());

            kafka.stop();
            await().until(() -> !kafka.isRunning());

            await().until(() -> !source.getConsumer().paused().await().indefinitely().isEmpty());
            subscriber.request(3);
            await().until(() -> subscriber.getItems().size() == 4);

            subscriber.request(10);
            AtomicInteger last = new AtomicInteger(subscriber.getItems().size());
            // Make sure we can't poll anymore.
            await()
                    .pollDelay(Duration.ofMillis(1000))
                    .until(() -> {
                        return last.get() == last.getAndSet(subscriber.getItems().size());
                    });

            try (StrimziKafkaContainer restarted = KafkaBrokerExtension.startKafkaBroker(port)) {
                await().until(restarted::isRunning);

                subscriber.request(100);
                await().until(() -> source.getConsumer().paused().await().indefinitely().isEmpty());

                sendMessages(10, 45, restarted.getBootstrapServers());
                await().until(() -> subscriber.getItems().size() == 55);
            }
        }
    }

    @Test
    public void testWithBrokerRestart() throws Exception {
        int sendBatchSize = 10;
        try (StrimziKafkaContainer kafka = KafkaBrokerExtension.createKafkaContainer()) {
            kafka.start();
            String groupId = UUID.randomUUID().toString();
            MapBasedConfig config = createConsumerConfig(groupId)
                    .put("topic", topic);

            KafkaSource<Integer, String> source = createSource(config, groupId);
            CountDownLatch receiveLatch = new CountDownLatch(sendBatchSize * 2);
            subscribe(source.getStream(), receiveLatch);
            sendMessages(0, sendBatchSize);
            try (StrimziKafkaContainer ignored = restart(kafka, 5)) {
                sendMessages(sendBatchSize, sendBatchSize);
                waitForMessages(receiveLatch);
                checkConsumedMessages();
            }
        }
    }
}
