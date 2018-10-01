package io.smallrye.reactive.messaging.kafka;

import org.eclipse.microprofile.reactive.messaging.Message;

public interface KafkaMessage<K, T> extends Message<T> {


  static <K, T> KafkaMessage<K, T> of(K key, T value) {
    return new SendingKafkaMessage<>(null, key, value, null, null, new MessageHeaders());
  }

  static <K, T> KafkaMessage<K, T> of(String topic, K key, T value) {
    return new SendingKafkaMessage<>(topic, key, value, null, null, new MessageHeaders());
  }

  static <K, T> KafkaMessage<K, T> of(String topic, K key, T value, Long timestamp, Integer partion) {
    return new SendingKafkaMessage<>(topic, key, value, timestamp, partion, new MessageHeaders());
  }

  T getPayload();

  K getKey();

  String getTopic();

  Integer getPartition();

  Long getTimestamp();

  MessageHeaders getHeaders();

}
