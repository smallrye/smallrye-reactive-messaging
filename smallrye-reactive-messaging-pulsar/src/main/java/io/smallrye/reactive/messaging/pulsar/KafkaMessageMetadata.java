package io.smallrye.reactive.messaging.kafka;

import java.time.Instant;

import org.apache.kafka.common.header.Headers;

public interface KafkaMessageMetadata<K> {

    String getTopic();

    K getKey();

    Instant getTimestamp();

    Headers getHeaders();

    int getPartition();

}
