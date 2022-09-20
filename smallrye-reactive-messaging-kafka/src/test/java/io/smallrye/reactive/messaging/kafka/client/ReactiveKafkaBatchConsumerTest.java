package io.smallrye.reactive.messaging.kafka.client;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.kafka.*;
import io.smallrye.reactive.messaging.kafka.base.SingletonInstance;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ReactiveKafkaBatchConsumerTest extends ClientTestBase {

    @Test
    public void testReception() throws Exception {
        Multi<IncomingKafkaRecordBatch<Integer, String>> stream = createSource().getBatchStream();
        sendReceive(stream, 0, 100, 0, 100, 50);
    }

    private void sendReceive(Multi<IncomingKafkaRecordBatch<Integer, String>> stream,
            int sendStartIndex, int sendCount,
            int receiveStartIndex, int receiveCount, int batchCount) throws Exception {

        CountDownLatch latch = new CountDownLatch(sendCount);
        subscribeBatch(stream, latch);
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
                commitHandlerFactories, failureHandlerFactories,
                listeners, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 0);
        return source;
    }

    private void subscribeBatch(Multi<IncomingKafkaRecordBatch<Integer, String>> stream, CountDownLatch... latches)
            throws Exception {
        Cancellable cancellable = stream
                .onItem().invoke(record -> {
                    onReceive(record);
                    for (CountDownLatch latch : latches) {
                        for (KafkaRecord<Integer, String> ignored : record.getRecords()) {
                            latch.countDown();
                        }
                    }
                })
                .subscribe().with(ignored -> {
                    // Ignored.
                });
        subscriptions.add(cancellable);
        waitForPartitionAssignment();
    }

}
