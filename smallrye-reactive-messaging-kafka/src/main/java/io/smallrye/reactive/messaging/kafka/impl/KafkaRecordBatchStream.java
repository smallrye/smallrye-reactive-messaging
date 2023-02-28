package io.smallrye.reactive.messaging.kafka.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.vertx.core.Context;

public class KafkaRecordBatchStream<K, V> extends AbstractMulti<ConsumerRecords<K, V>> {

    private final ReactiveKafkaConsumer<K, V> client;
    private final RuntimeKafkaSourceConfiguration config;
    private final Context context;
    private final Set<KafkaRecordStreamSubscription<K, V, ConsumerRecords<K, V>>> subscriptions = Collections
            .newSetFromMap(new ConcurrentHashMap<>());

    public KafkaRecordBatchStream(ReactiveKafkaConsumer<K, V> client,
            RuntimeKafkaSourceConfiguration config, Context context) {
        this.config = config;
        this.client = client;
        this.context = context;
    }

    @Override
    public void subscribe(MultiSubscriber<? super ConsumerRecords<K, V>> subscriber) {
        // Enqueue ConsumerRecords by batches, max poll records is considered 1
        KafkaRecordStreamSubscription<K, V, ConsumerRecords<K, V>> subscription = new KafkaRecordStreamSubscription<>(
                client, config, subscriber, context, 1, (cr, q) -> q.offer(cr));
        subscriptions.add(subscription);
        subscriber.onSubscribe(subscription);
    }

    void removeFromQueueRecordsFromTopicPartitions(Collection<TopicPartition> revokedPartitions) {
        if (revokedPartitions.isEmpty()) {
            return;
        }
        subscriptions
                .forEach(s -> this.removeFromQueue(s, revokedPartitions));
    }

    private void removeFromQueue(KafkaRecordStreamSubscription<K, V, ConsumerRecords<K, V>> subscription,
            Collection<TopicPartition> revokedPartitions) {
        subscription.rewriteQueue(cr -> {
            Map<TopicPartition, List<ConsumerRecord<K, V>>> records = new HashMap<>();
            cr.partitions()
                    .stream()
                    .filter(t -> !revokedPartitions.contains(t))
                    .forEach(t -> records.put(t, cr.records(t)));

            if (!records.isEmpty()) {
                // replace ConsumerRecords with the new one
                return new ConsumerRecords<>(records);
            } else {
                // remove from the queue
                return null;
            }
        });
    }

}
