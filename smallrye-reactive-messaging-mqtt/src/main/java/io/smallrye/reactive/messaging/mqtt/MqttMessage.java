package io.smallrye.reactive.messaging.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import org.eclipse.microprofile.reactive.messaging.Message;

public interface MqttMessage extends Message<byte[]> {

  static MqttMessage of(byte[] payload) {
    return new SendingMqttMessage(null, payload, null, false);
  }

  static MqttMessage of( String topic, byte[] payload) {
    return new SendingMqttMessage(topic, payload, null, false);
  }

  static MqttMessage of( String topic, byte[] payload, MqttQoS qos) {
    return new SendingMqttMessage(topic, payload, qos, false);
  }

  static MqttMessage of( String topic, byte[] payload, MqttQoS qos, boolean retain) {
    return new SendingMqttMessage(topic, payload, qos, retain);
  }

  int getMessageId();

  MqttQoS getQosLevel();

  boolean isDuplicate();

  boolean isRetain();

  String getTopic();
}
