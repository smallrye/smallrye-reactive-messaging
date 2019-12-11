package io.smallrye.reactive.messaging.kafka;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.eclipse.microprofile.reactive.messaging.Headers;

import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.reactivex.kafka.client.producer.KafkaHeader;

public class ReceivedKafkaMessage<K, T> implements KafkaMessage<K, T> {

    private final KafkaConsumerRecord<K, T> record;
    private final KafkaConsumer<K, T> consumer;
    private final Headers headers;

    public ReceivedKafkaMessage(KafkaConsumer<K, T> consumer, KafkaConsumerRecord<K, T> record) {
        this.record = Objects.requireNonNull(record);
        this.consumer = Objects.requireNonNull(consumer);
        Headers.HeadersBuilder builder = Headers.builder();
        if (record.key() != null) {
            builder.with(KafkaHeaders.KEY, record.key());
        }
        if (record.topic() != null) {
            builder.with(KafkaHeaders.TOPIC, record.topic());
        }
        if (record.partition() >= 0) {
            builder.with(KafkaHeaders.PARTITION, record.partition());
        }
        if (record.timestamp() >= 0) {
            builder.with(KafkaHeaders.TIMESTAMP, record.timestamp());
        }
        if (record.offset() >= 0) {
            builder.with(KafkaHeaders.OFFSET, record.offset());
        }
        List<KafkaHeader> recordHeaders = record.headers();
        if (recordHeaders != null) {
            // We get Vert.x Kafka Header (<String, Buffer>), to avoid leaking this class,
            // we recreate the Kafka Record Header object. Unfortunately, we cannot get the original one.
            // It's lost in the Vert.x translation.

            // Be aware, we store the Kafka Record headers, not the MessageHeaders structure
            List<RecordHeader> list = recordHeaders.stream()
                    .map(vertxKafkaHeader -> new RecordHeader(vertxKafkaHeader.key(), vertxKafkaHeader.value().getBytes()))
                    .collect(Collectors.toList());
            builder.with(KafkaHeaders.KAFKA_HEADERS, Collections.unmodifiableList(list));
        }
        if (record.timestampType() != null) {
            builder.with(KafkaHeaders.KAFKA_TIMESTAMP_TYPE, record.timestampType());
        }
        this.headers = builder.build();
    }

    @Override
    public T getPayload() {
        return record.value();
    }

    @Override
    public K getKey() {
        return record.key();
    }

    @Override
    public String getTopic() {
        return record.topic();
    }

    @Override
    public int getPartition() {
        return record.partition();
    }

    @Override
    public long getTimestamp() {
        return record.timestamp();
    }

    @Override
    public long getOffset() {
        return record.offset();
    }

    @SuppressWarnings("unchecked")
    @Override
    public MessageHeaders getKafkaHeaders() {
        Iterable<Header> iterable = headers.get(KafkaHeaders.KAFKA_HEADERS, Iterable.class);
        if (iterable != null) {
            return new MessageHeaders(iterable);
        } else {
            return new MessageHeaders(Collections.emptyList());
        }
    }

    @Override
    public Headers getHeaders() {
        return headers;
    }

    @Override
    public Supplier<CompletionStage<Void>> getAckSupplier() {
        return this::ack;
    }

    public ConsumerRecord unwrap() {
        return record.getDelegate().record();
    }

    @Override
    public CompletionStage<Void> ack() {
        consumer.commit();
        return CompletableFuture.completedFuture(null);
    }
}
