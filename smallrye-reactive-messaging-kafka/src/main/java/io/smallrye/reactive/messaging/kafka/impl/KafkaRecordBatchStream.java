package io.smallrye.reactive.messaging.kafka.impl;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.vertx.core.Context;

public class KafkaRecordBatchStream<K, V> extends AbstractMulti<ConsumerRecords<K, V>> {

    private final ReactiveKafkaConsumer<K, V> client;
    private final KafkaConnectorIncomingConfiguration config;
    private final Context context;

    public KafkaRecordBatchStream(ReactiveKafkaConsumer<K, V> client,
            KafkaConnectorIncomingConfiguration config, Context context) {
        this.config = config;
        this.client = client;
        this.context = context;
    }

    @Override
    public void subscribe(MultiSubscriber<? super ConsumerRecords<K, V>> subscriber) {
        // Enqueue ConsumerRecords by batches, max poll records is considered 1
        KafkaRecordStreamSubscription<K, V, ConsumerRecords<K, V>> subscription = new KafkaRecordStreamSubscription<>(
                client, config, subscriber, context, 1, (cr, q) -> q.offer(cr));
        subscriber.onSubscribe(subscription);
    }

}
