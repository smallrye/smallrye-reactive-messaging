package io.smallrye.reactive.messaging.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;

public final class SendingMqttMessage implements MqttMessage {

  private final String topic;
  private final byte[] payload;
  private final MqttQoS qos;
  private final boolean isRetain;

  SendingMqttMessage(String topic, byte[] payload, MqttQoS qos, boolean isRetain) {
    this.topic = topic;
    this.payload = payload;
    this.qos = qos;
    this.isRetain = isRetain;
  }

  @Override
  public byte[] getPayload() {
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
