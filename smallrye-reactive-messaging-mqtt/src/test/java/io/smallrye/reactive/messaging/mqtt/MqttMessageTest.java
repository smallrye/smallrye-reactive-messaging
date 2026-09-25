package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

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

    @Test
    void testOfResponsePropagatesResponseTopicQosAndCorrelationData() {
        MqttMessage<byte[]> request = incomingRequest("response/topic", new byte[] { 1, 2, 3 });

        MqttMessage<String> response = MqttMessage.ofResponse(request, "pong", MqttQoS.AT_LEAST_ONCE);

        assertThat(response.getPayload()).isEqualTo("pong");
        assertThat(response.getTopic()).isEqualTo("response/topic");
        assertThat(response.getQosLevel()).isEqualTo(MqttQoS.AT_LEAST_ONCE);
        assertThat(metadataOf(response).getCorrelationData()).containsExactly(1, 2, 3);
    }

    @Test
    void testOfResponseWithoutCorrelationData() {
        MqttMessage<byte[]> request = incomingRequest("response/topic", null);

        MqttMessage<String> response = MqttMessage.ofResponse(request, "pong", MqttQoS.AT_MOST_ONCE);

        assertThat(metadataOf(response).getCorrelationData()).isNull();
    }

    @Test
    void testOfResponseWithUserProperties() {
        MqttMessage<byte[]> request = incomingRequest("response/topic", null);

        MqttMessage<String> response = MqttMessage.ofResponse(request, "pong", MqttQoS.AT_MOST_ONCE,
                Map.of("version", "2"));

        assertThat(metadataOf(response).getUserProperties()).containsEntry("version", "2");
    }

    @Test
    void testOfResponseWithoutResponseTopic() {
        MqttMessage<byte[]> request = incomingRequest(null, null);

        assertThatThrownBy(() -> MqttMessage.ofResponse(request, "pong", MqttQoS.AT_MOST_ONCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Response Topic");
    }

    private static SendingMqttMessageMetadata metadataOf(MqttMessage<?> message) {
        return message.getMetadata(SendingMqttMessageMetadata.class).orElseThrow();
    }

    private static MqttMessage<byte[]> incomingRequest(String responseTopic, byte[] correlationData) {
        return new MqttMessage<>() {
            @Override
            public byte[] getPayload() {
                return new byte[0];
            }

            @Override
            public int getMessageId() {
                return -1;
            }

            @Override
            public MqttQoS getQosLevel() {
                return MqttQoS.AT_MOST_ONCE;
            }

            @Override
            public boolean isDuplicate() {
                return false;
            }

            @Override
            public boolean isRetain() {
                return false;
            }

            @Override
            public String getTopic() {
                return "request/topic";
            }

            @Override
            public String getResponseTopic() {
                return responseTopic;
            }

            @Override
            public byte[] getCorrelationData() {
                return correlationData;
            }
        };
    }
}
