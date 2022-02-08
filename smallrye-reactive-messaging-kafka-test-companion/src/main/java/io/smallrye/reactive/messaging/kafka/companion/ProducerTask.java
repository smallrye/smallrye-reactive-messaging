package io.smallrye.reactive.messaging.kafka.companion;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.Multi;

/**
 * Task for producing Kafka records provided by the given {@link Multi}
 */
public class ProducerTask extends KafkaTask<RecordMetadata, ProducerTask> {

    /**
     * Creates {@link ProducerTask}
     *
     * @param multi the multi providing produced {@link RecordMetadata}
     */
    public ProducerTask(Multi<RecordMetadata> multi) {
        super(multi);
    }

    @Override
    protected long offset(RecordMetadata record) {
        return record.offset();
    }

    @Override
    protected TopicPartition topicPartition(RecordMetadata record) {
        return new TopicPartition(record.topic(), record.partition());
    }
}
