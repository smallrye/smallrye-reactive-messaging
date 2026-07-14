package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.netty.handler.codec.mqtt.MqttProperties;
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
        byte[] correlation = new byte[] { 1, 2, 3 };
        MqttMessage<byte[]> incoming = fakeIncoming("response/topic", correlation);

        MqttMessage<String> reply = MqttMessage.ofResponse(incoming, "pong", MqttQoS.AT_LEAST_ONCE);

        assertThat(reply.getPayload()).isEqualTo("pong");
        assertThat(reply.getTopic()).isEqualTo("response/topic");
        assertThat(reply.getQosLevel()).isEqualTo(MqttQoS.AT_LEAST_ONCE);

        SendingMqttMessageMetadata md = reply.getMetadata(SendingMqttMessageMetadata.class).orElseThrow();
        MqttProperties.MqttProperty<?> cd = md.getProperties()
                .getProperty(MqttProperties.MqttPropertyType.CORRELATION_DATA.value());
        assertThat(cd).isNotNull();
        assertThat((byte[]) cd.value()).containsExactly(1, 2, 3);
    }

    @Test
    void testOfResponseOmitsCorrelationDataWhenAbsent() {
        MqttMessage<byte[]> incoming = fakeIncoming("response/topic", null);

        MqttMessage<String> reply = MqttMessage.ofResponse(incoming, "pong", MqttQoS.AT_MOST_ONCE);

        SendingMqttMessageMetadata md = reply.getMetadata(SendingMqttMessageMetadata.class).orElseThrow();
        assertThat(md.getProperties()
                .getProperty(MqttProperties.MqttPropertyType.CORRELATION_DATA.value())).isNull();
    }

    @Test
    void testOfResponseThrowsWhenResponseTopicMissing() {
        MqttMessage<byte[]> incoming = fakeIncoming(null, null);

        assertThatThrownBy(() -> MqttMessage.ofResponse(incoming, "pong", MqttQoS.AT_MOST_ONCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Response Topic");
    }

    @Test
    void testOfResponseDoesNotMutateCallerProperties() {
        MqttMessage<byte[]> incoming = fakeIncoming("response/topic", new byte[] { 9 });
        MqttProperties callerProps = new MqttProperties();

        MqttMessage.ofResponse(incoming, "pong", MqttQoS.AT_MOST_ONCE, callerProps);

        assertThat(callerProps.listAll()).isEmpty();
    }

    private static MqttMessage<byte[]> fakeIncoming(String responseTopic, byte[] correlationData) {
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
