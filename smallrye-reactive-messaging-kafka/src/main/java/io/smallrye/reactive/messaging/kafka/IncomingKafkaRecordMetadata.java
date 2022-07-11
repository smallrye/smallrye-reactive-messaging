package io.smallrye.reactive.messaging.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @deprecated use {@link io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata} instead
 */
@Deprecated
public class IncomingKafkaRecordMetadata<K, T>
        extends io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata<K, T> implements KafkaMessageMetadata<K> {

    public IncomingKafkaRecordMetadata(ConsumerRecord<K, T> record, String channelName, int index) {
        super(record, channelName, index);
    }

    public IncomingKafkaRecordMetadata(ConsumerRecord<K, T> record, String channelName) {
        super(record, channelName, -1);
    }

}
