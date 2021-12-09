package io.smallrye.reactive.messaging.kafka.companion;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.Multi;

/**
 * Task for consumer Kafka records provided by the given {@link Multi}
 *
 * @param <K> The record key type
 * @param <V> The record value type
 */
public class ConsumerTask<K, V> extends KafkaTask<ConsumerRecord<K, V>, ConsumerTask<K, V>> {

    /**
     * Create {@link ConsumerTask}
     *
     * @param multi multi providing {@link ConsumerRecord}s
     */
    public ConsumerTask(Multi<ConsumerRecord<K, V>> multi) {
        super(multi.broadcast()
                .withCancellationAfterLastSubscriberDeparture()
                .toAllSubscribers());
    }

    @Override
    protected long offset(ConsumerRecord<K, V> record) {
        return record.offset();
    }

    @Override
    protected TopicPartition topicPartition(ConsumerRecord<K, V> record) {
        return new TopicPartition(record.topic(), record.partition());
    }
}
