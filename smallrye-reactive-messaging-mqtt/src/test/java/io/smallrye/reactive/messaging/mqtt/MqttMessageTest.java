package io.smallrye.reactive.messaging.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MqttMessageTest {

  @Test
  public void testOfPayloadOnly() {
    MqttMessage message = MqttMessage.of( "test".getBytes() );

    assertThat(new String(message.getPayload())).isEqualTo("test");
    assertThat(message.getTopic()).isNull();
    assertThat(message.getMessageId()).isEqualTo(-1);
    assertThat(message.getQosLevel()).isNull();
    assertThat(message.isDuplicate()).isFalse();
    assertThat(message.isRetain()).isFalse();
  }

  @Test
  public void testOfTopicAndPayload() {
    MqttMessage message = MqttMessage.of( "topic1", "testWithTopic".getBytes() );

    assertThat(new String(message.getPayload())).isEqualTo("testWithTopic");
    assertThat(message.getTopic()).isEqualTo("topic1");
    assertThat(message.getMessageId()).isEqualTo(-1);
    assertThat(message.getQosLevel()).isNull();
    assertThat(message.isDuplicate()).isFalse();
    assertThat(message.isRetain()).isFalse();
  }

  @Test
  public void testOfTopicPayloadAndQos() {
    MqttMessage message = MqttMessage.of( "topic2", "testWithQos".getBytes(), MqttQoS.EXACTLY_ONCE );

    assertThat(new String(message.getPayload())).isEqualTo("testWithQos");
    assertThat(message.getTopic()).isEqualTo("topic2");
    assertThat(message.getMessageId()).isEqualTo(-1);
    assertThat(message.getQosLevel()).isEqualTo(MqttQoS.EXACTLY_ONCE);
    assertThat(message.isDuplicate()).isFalse();
    assertThat(message.isRetain()).isFalse();
  }

  @Test
  public void testOfTopicPayloadQosAndRetain() {
    MqttMessage message = MqttMessage.of( "topic3", "testWithRetain".getBytes(), MqttQoS.EXACTLY_ONCE, true);

    assertThat(new String(message.getPayload())).isEqualTo("testWithRetain");
    assertThat(message.getTopic()).isEqualTo("topic3");
    assertThat(message.getMessageId()).isEqualTo(-1);
    assertThat(message.getQosLevel()).isEqualTo(MqttQoS.EXACTLY_ONCE);
    assertThat(message.isDuplicate()).isFalse();
    assertThat(message.isRetain()).isTrue();
  }
}
