package io.smallrye.reactive.messaging.kafka.impl;

import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.vertx.core.Context;

public class KafkaRecordStream<K, V> extends AbstractMulti<ConsumerRecord<K, V>> {

    private final ReactiveKafkaConsumer<K, V> client;
    private final KafkaConnectorIncomingConfiguration config;
    private final Context context;

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
        subscriber.onSubscribe(subscription);
    }

}
