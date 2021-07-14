package io.smallrye.reactive.messaging.kafka.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordBatch;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.base.SingletonInstance;
import io.smallrye.reactive.messaging.kafka.base.TopicHelpers;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ReactiveKafkaBatchConsumerTest extends ClientTestBase {

    private final Semaphore assignSemaphore = new Semaphore(partitions);
    private final List<Cancellable> subscriptions = new ArrayList<>();
    private KafkaSource<Integer, String> source;

    @AfterEach
    public void tearDown() {
        cancelSubscriptions();
        source.closeQuietly();
    }

    @BeforeEach
    public void init() {
        topic = TopicHelpers.createNewTopic("test-" + UUID.randomUUID().toString(), partitions);
        resetMessages();
    }

    private void cancelSubscriptions() {
        subscriptions.forEach(Cancellable::cancel);
        subscriptions.clear();
    }

    @Test
    public void testReception() throws Exception {
        Multi<IncomingKafkaRecordBatch<Integer, String>> stream = createSource().getBatchStream();
        sendReceive(stream, 0, 100, 0, 100, 50);
    }

    private void sendReceive(Multi<IncomingKafkaRecordBatch<Integer, String>> stream,
            int sendStartIndex, int sendCount,
            int receiveStartIndex, int receiveCount, int batchCount) throws Exception {

        CountDownLatch latch = new CountDownLatch(batchCount);
        subscribe(stream, latch);
        if (sendCount > 0) {
            sendMessages(sendStartIndex, sendCount);
        }
        waitForMessages(latch);
        checkConsumedMessages(receiveStartIndex, receiveCount);
    }

    public KafkaSource<Integer, String> createSource() {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .put("topic", topic)
                .put("batch", true);

        return createSource(config, groupId);
    }

    public KafkaSource<Integer, String> createSource(MapBasedConfig config, String groupId) {
        SingletonInstance<KafkaConsumerRebalanceListener> listeners = new SingletonInstance<>(groupId,
                getKafkaConsumerRebalanceListenerAwaitingAssignation());

        source = new KafkaSource<>(vertx, groupId, new KafkaConnectorIncomingConfiguration(config),
                listeners, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 0);
        return source;
    }

    private KafkaConsumerRebalanceListener getKafkaConsumerRebalanceListenerAwaitingAssignation() {
        return new KafkaConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer,
                    Collection<TopicPartition> partitions) {
                ReactiveKafkaBatchConsumerTest.this.onPartitionsAssigned(partitions);
            }
        };
    }

    private void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        assertThat(topic).isEqualTo(partitions.iterator().next().topic());
        assignSemaphore.release(partitions.size());
    }

    private void waitForMessages(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS)) {
            fail(latch.getCount() + " messages not received, received=" + count(receivedMessages) + " : "
                    + receivedMessages);
        }
    }

    private void subscribe(Multi<IncomingKafkaRecordBatch<Integer, String>> stream, CountDownLatch... latches)
            throws Exception {
        Cancellable cancellable = stream
                .onItem().invoke(record -> {
                    onReceive(record);
                    for (CountDownLatch latch : latches) {
                        latch.countDown();
                    }
                })
                .subscribe().with(ignored -> {
                    // Ignored.
                });
        subscriptions.add(cancellable);
        waitFoPartitionAssignment();
    }

    private void waitFoPartitionAssignment() throws InterruptedException {
        assertTrue("Partitions not assigned",
                assignSemaphore.tryAcquire(sessionTimeoutMillis + 1000, TimeUnit.MILLISECONDS));
    }

}
