package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

public class KafkaMessageTest {

    @Test
    public void testCreationOfKafkaMessageFromKeyAndValue() {
        KafkaMessage<String, String> message = KafkaMessage.of("foo", "bar");
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isEqualTo("foo");
        assertThat(message.getTopic()).isNull();
        assertThat(message.getMessageHeaders().unwrap()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(-1);
        assertThat(message.getTimestamp()).isEqualTo(-1);
        assertThat(message.getOffset()).isEqualTo(-1);
    }

    @Test
    public void testCreationOfKafkaMessageFromKeyAndValueAndTopic() {
        KafkaMessage<String, String> message = KafkaMessage.of("topic", "foo", "bar");
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isEqualTo("foo");
        assertThat(message.getTopic()).isEqualTo("topic");
        assertThat(message.getMessageHeaders().unwrap()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(-1);
        assertThat(message.getTimestamp()).isEqualTo(-1);
    }

    @Test
    public void testCreationOfKafkaMessageWithEverything() {
        long timestamp = System.currentTimeMillis();
        KafkaMessage<String, String> message = KafkaMessage.of("topic", "foo", "bar", timestamp, 2);
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isEqualTo("foo");
        assertThat(message.getTopic()).isEqualTo("topic");
        assertThat(message.getMessageHeaders().unwrap()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(2);
        assertThat(message.getTimestamp()).isEqualTo(timestamp);
    }

    @Test
    public void testCreationOfKafkaMessageWithEverythingButWithNullValues() {
        KafkaMessage<String, String> message = KafkaMessage.of(null, null, "bar", -1, -1);
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isNull();
        assertThat(message.getTopic()).isNull();
        assertThat(message.getMessageHeaders().unwrap()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(-1);
        assertThat(message.getTimestamp()).isEqualTo(-1);
    }

    @Test
    public void testHeaders() {
        KafkaMessage<String, String> message = KafkaMessage.of("a", "b");
        KafkaMessage<String, String> message1 = message.withHeader("x-key", "value", StandardCharsets.UTF_8);
        KafkaMessage<String, String> message2 = message1.withHeader("x-key-2", "value-2".getBytes());

        assertThat(message).isNotEqualTo(message1).isNotEqualTo(message2);
        assertThat(message1).isNotEqualTo(message2);

        MessageHeaders headers = message2.getMessageHeaders();
        assertThat(headers.getOneAsString("x-key")).hasValue("value");
        assertThat(headers.getOneAsBytes("x-key-2")).hasValue("value-2".getBytes());
    }

}
