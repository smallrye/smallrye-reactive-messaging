package io.smallrye.reactive.messaging.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.reactivex.mqtt.messages.MqttPublishMessage;
import org.eclipse.microprofile.reactive.messaging.Message;

public class ReceivingMqttMessage implements MqttMessage {
  final MqttPublishMessage message;

  ReceivingMqttMessage( MqttPublishMessage message ) {
    this.message = message;
  }

  @Override
  public byte[] getPayload() {
    return this.message.payload().getDelegate().getBytes();
  }

  public int getMessageId() {
    return message.messageId();
  }

  public MqttQoS getQosLevel() {
    return message.qosLevel();
  }

  public boolean isDuplicate() {
    return message.isDup();
  }

  public boolean isRetain() {
    return message.isRetain();
  }

  public String getTopic() {
    return message.topicName();
  }
}
