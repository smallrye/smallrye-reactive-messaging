package io.smallrye.reactive.messaging.kafka;

import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class KafkaMessage<K, T> implements Message<T> {

  private final KafkaConsumerRecord<K, T> record;
  private final KafkaConsumer<K, T> consumer;

  public KafkaMessage(KafkaConsumer<K, T> consumer, KafkaConsumerRecord<K, T> record) {
    this.record = Objects.requireNonNull(record);
    this.consumer = Objects.requireNonNull(consumer);
  }

  @Override
  public T getPayload() {
    return record.value();
  }

  public K getKey() {
    return record.key();
  }

  public KafkaConsumerRecord getRecord() {
    return record;
  }

  public String getTopic() {
    return record.topic();
  }

  public int getPartition() {
    return record.partition();
  }

  public long getOffset() {
    return record.offset();
  }

  @Override
  public CompletionStage<Void> ack() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    consumer.commit(v -> {
      if (v.succeeded()) {
        future.complete(null);
      } else {
        future.completeExceptionally(v.cause());
      }
    });
    return future;
  }


}
