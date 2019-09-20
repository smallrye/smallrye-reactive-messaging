package io.smallrye.reactive.messaging.kafka;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.opentracing.Span;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;

public class ReceivedKafkaMessage<K, T> implements KafkaMessage<K, T> {

    private final KafkaConsumerRecord<K, T> record;
    private final KafkaConsumer<K, T> consumer;
    private final MessageHeaders headers;
    private final Span span;

    public ReceivedKafkaMessage(KafkaConsumer<K, T> consumer, KafkaConsumerRecord<K, T> record, Span span) {
        this.record = Objects.requireNonNull(record);
        this.consumer = Objects.requireNonNull(consumer);
        this.headers = new MessageHeaders(record.getDelegate().record().headers());
        this.span = span;
    }

    @Override
    public T getPayload() {
        return record.value();
    }

    public K getKey() {
        return record.key();
    }

    public String getTopic() {
        return record.topic();
    }

    public Integer getPartition() {
        return record.partition();
    }

    @Override
    public Long getTimestamp() {
        return record.timestamp();
    }

    @Override
    public MessageHeaders getHeaders() {
        return headers;
    }

    public ConsumerRecord unwrap() {
        return record.getDelegate().record();
    }

    @Override
    public CompletionStage<Void> ack() {
        consumer.commit();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Span getSpan() {
        return span;
    }
}
