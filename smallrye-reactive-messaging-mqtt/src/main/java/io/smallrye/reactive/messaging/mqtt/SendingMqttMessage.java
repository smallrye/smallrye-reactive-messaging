package io.smallrye.reactive.messaging.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;

public final class SendingMqttMessage<T> implements MqttMessage<T> {

  private final String topic;
  private final T payload;
  private final MqttQoS qos;
  private final boolean isRetain;

  SendingMqttMessage(String topic, T payload, MqttQoS qos, boolean isRetain) {
    this.topic = topic;
    this.payload = payload;
    this.qos = qos;
    this.isRetain = isRetain;
  }

  public T getPayload() {
    return payload;
  }

  public int getMessageId() {
    return -1;
  }

  public MqttQoS getQosLevel() {
    return qos;
  }

  public boolean isDuplicate() {
    return false;
  }

  public boolean isRetain() {
    return isRetain;
  }

  public String getTopic() {
    return topic;
  }
}
