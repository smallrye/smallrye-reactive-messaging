package io.smallrye.reactive.messaging.kafka;

import org.apache.kafka.common.header.Headers;

public interface KafkaMessageMetadata<K> {

    String getTopic();

    K getKey();

    long getTimestamp();

    Headers getHeaders();

    int getPartition();

}
