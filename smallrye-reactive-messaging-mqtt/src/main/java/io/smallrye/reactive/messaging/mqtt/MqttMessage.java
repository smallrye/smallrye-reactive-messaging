package io.smallrye.reactive.messaging.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import org.eclipse.microprofile.reactive.messaging.Message;

public interface MqttMessage<T> extends Message<T> {

  static <T> MqttMessage<T> of(T payload) {
    return new SendingMqttMessage(null, payload, null, false);
  }

  static <T> MqttMessage<T> of( String topic, T payload) {
    return new SendingMqttMessage(topic, payload, null, false);
  }

  static <T> MqttMessage<T> of( String topic, T payload, MqttQoS qos) {
    return new SendingMqttMessage(topic, payload, qos, false);
  }

  static <T> MqttMessage<T> of( String topic, T payload, MqttQoS qos, boolean retain) {
    return new SendingMqttMessage(topic, payload, qos, retain);
  }

  int getMessageId();

  MqttQoS getQosLevel();

  boolean isDuplicate();

  boolean isRetain();

  String getTopic();
}
