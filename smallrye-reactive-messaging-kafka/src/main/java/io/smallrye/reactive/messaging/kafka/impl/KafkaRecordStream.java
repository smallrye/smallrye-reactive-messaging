package io.smallrye.reactive.messaging.kafka.impl;

import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.vertx.core.Context;

public class KafkaRecordStream<K, V> extends AbstractMulti<ConsumerRecord<K, V>> {

    private final ReactiveKafkaConsumer<K, V> client;
    private final KafkaConnectorIncomingConfiguration config;
    private final Context context;
    private final Set<KafkaRecordStreamSubscription> subscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public KafkaRecordStream(ReactiveKafkaConsumer<K, V> client,
            KafkaConnectorIncomingConfiguration config, Context context) {
        this.config = config;
        this.client = client;
        this.context = context;
    }

    @Override
    public void subscribe(
            MultiSubscriber<? super ConsumerRecord<K, V>> subscriber) {
        // Kafka also defaults to 500, but doesn't have a constant for it
        int maxPollRecords = config.config().getOptionalValue(MAX_POLL_RECORDS_CONFIG, Integer.class).orElse(500);
        KafkaRecordStreamSubscription<K, V, ConsumerRecord<K, V>> subscription = new KafkaRecordStreamSubscription<>(
                client, config, subscriber, context, maxPollRecords, (cr, q) -> q.addAll(cr));
        subscriptions.add(subscription);
        subscriber.onSubscribe(subscription);
    }

    void removeFromQueueRecordsFromTopicPartitions(Collection<TopicPartition> partitions) {
        subscriptions
                .forEach(s -> this.removeFromQueue(s.getQueue(), partitions));
    }

    private void removeFromQueue(RecordQueue<ConsumerRecord<K, V>> queue, Collection<TopicPartition> partitions) {
        Map<String, Set<Integer>> revoked = new HashMap<>();
        partitions
                .forEach(topicPartition -> revoked
                        .computeIfAbsent(topicPartition.topic(), t -> new HashSet<>())
                        .add(topicPartition.partition()));

        synchronized (queue) {
            Iterator<ConsumerRecord<K, V>> iterator = queue.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<K, V> cr = iterator.next();
                Set<Integer> revokedPartitions = revoked.get(cr.topic());
                if (revokedPartitions != null && revokedPartitions.contains(cr.partition())) {
                    iterator.remove();
                }
            }
        }
    }
}
