package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.netty.handler.codec.mqtt.MqttQoS;

public class MqttMessageTest {

    @Test
    public void testOfPayloadOnly() {
        MqttMessage<String> message = MqttMessage.of("test");

        assertThat(message.getPayload()).isEqualTo("test");
        assertThat(message.getTopic()).isNull();
        assertThat(message.getMessageId()).isEqualTo(-1);
        assertThat(message.getQosLevel()).isNull();
        assertThat(message.isDuplicate()).isFalse();
        assertThat(message.isRetain()).isFalse();
    }

    @Test
    public void testOfPayloadOnlyInt() {
        MqttMessage<Integer> message = MqttMessage.of(42);

        assertThat(message.getPayload()).isEqualTo(42);
        assertThat(message.getTopic()).isNull();
        assertThat(message.getMessageId()).isEqualTo(-1);
        assertThat(message.getQosLevel()).isNull();
        assertThat(message.isDuplicate()).isFalse();
        assertThat(message.isRetain()).isFalse();
    }

    @Test
    public void testOfTopicAndPayload() {
        MqttMessage<String> message = MqttMessage.of("topic1", "testWithTopic");

        assertThat(message.getPayload()).isEqualTo("testWithTopic");
        assertThat(message.getTopic()).isEqualTo("topic1");
        assertThat(message.getMessageId()).isEqualTo(-1);
        assertThat(message.getQosLevel()).isNull();
        assertThat(message.isDuplicate()).isFalse();
        assertThat(message.isRetain()).isFalse();
    }

    @Test
    public void testOfTopicPayloadAndQos() {
        MqttMessage<String> message = MqttMessage.of("topic2", "testWithQos",
                MqttQoS.EXACTLY_ONCE);

        assertThat(message.getPayload()).isEqualTo("testWithQos");
        assertThat(message.getTopic()).isEqualTo("topic2");
        assertThat(message.getMessageId()).isEqualTo(-1);
        assertThat(message.getQosLevel()).isEqualTo(MqttQoS.EXACTLY_ONCE);
        assertThat(message.isDuplicate()).isFalse();
        assertThat(message.isRetain()).isFalse();
    }

    @Test
    public void testOfTopicPayloadQosAndRetain() {
        MqttMessage<String> message = MqttMessage.of("topic3", "testWithRetain",
                MqttQoS.EXACTLY_ONCE, true);

        assertThat(message.getPayload()).isEqualTo("testWithRetain");
        assertThat(message.getTopic()).isEqualTo("topic3");
        assertThat(message.getMessageId()).isEqualTo(-1);
        assertThat(message.getQosLevel()).isEqualTo(MqttQoS.EXACTLY_ONCE);
        assertThat(message.isDuplicate()).isFalse();
        assertThat(message.isRetain()).isTrue();
    }

    @Test
    void testOfMetadata() {
        MqttMessage<String> message = MqttMessage.of(
                new SendingMqttMessageMetadata("topic3", MqttQoS.EXACTLY_ONCE, true),
                "testWithRetain");

        assertThat(message.getPayload()).isEqualTo("testWithRetain");
        assertThat(message.getTopic()).isEqualTo("topic3");
        assertThat(message.getMessageId()).isEqualTo(-1);
        assertThat(message.getQosLevel()).isEqualTo(MqttQoS.EXACTLY_ONCE);
        assertThat(message.isDuplicate()).isFalse();
        assertThat(message.isRetain()).isTrue();
    }
}
