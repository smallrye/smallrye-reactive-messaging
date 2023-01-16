package io.smallrye.reactive.messaging.kafka.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.vertx.core.Context;

public class KafkaRecordStream<K, V> extends AbstractMulti<ConsumerRecord<K, V>> {

    private final ReactiveKafkaConsumer<K, V> client;
    private final RuntimeKafkaSourceConfiguration config;
    private final Context context;
    private final Set<KafkaRecordStreamSubscription<K, V, ConsumerRecord<K, V>>> subscriptions = Collections
            .newSetFromMap(new ConcurrentHashMap<>());

    public KafkaRecordStream(ReactiveKafkaConsumer<K, V> client,
            RuntimeKafkaSourceConfiguration config, Context context) {
        this.config = config;
        this.client = client;
        this.context = context;
    }

    @Override
    public void subscribe(
            MultiSubscriber<? super ConsumerRecord<K, V>> subscriber) {
        // Kafka also defaults to 500, but doesn't have a constant for it
        int maxPollRecords = config.getMaxPollRecords();
        KafkaRecordStreamSubscription<K, V, ConsumerRecord<K, V>> subscription = new KafkaRecordStreamSubscription<>(
                client, config, subscriber, context, maxPollRecords, (cr, q) -> q.addAll(cr));
        subscriptions.add(subscription);
        subscriber.onSubscribe(subscription);
    }

    void removeFromQueueRecordsFromTopicPartitions(Collection<TopicPartition> revokedPartitions) {
        if (revokedPartitions.isEmpty()) {
            return;
        }
        Map<String, Set<Integer>> revoked = new HashMap<>();
        revokedPartitions
                .forEach(topicPartition -> revoked
                        .computeIfAbsent(topicPartition.topic(), t -> new HashSet<>())
                        .add(topicPartition.partition()));

        subscriptions
                .forEach(s -> this.removeFromQueue(s, revoked));
    }

    private void removeFromQueue(
            KafkaRecordStreamSubscription<K, V, ConsumerRecord<K, V>> subscription,
            Map<String, Set<Integer>> revoked) {
        subscription
                .rewriteQueue(
                        cr -> {
                            Set<Integer> revokedPartitions = revoked.get(cr.topic());
                            if (revokedPartitions != null && revokedPartitions.contains(cr.partition())) {
                                // remove from the queue
                                return null;
                            } else {
                                return cr;
                            }
                        });
    }
}
