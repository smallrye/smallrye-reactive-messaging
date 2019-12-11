package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.reactive.messaging.Headers;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.Test;

public class KafkaMessageTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testCreationOfKafkaMessageFromKeyAndValue() {
        KafkaMessage<String, String> message = KafkaMessage.of("foo", "bar");
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isEqualTo("foo");
        assertThat(message.getTopic()).isNull();
        assertThat(message.getKafkaHeaders().unwrap()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(-1);
        assertThat(message.getTimestamp()).isEqualTo(-1);
        assertThat(message.getOffset()).isEqualTo(-1);

        Headers headers = message.getHeaders();
        assertThat(headers.getAsInteger(KafkaHeaders.PARTITION, -1)).isEqualTo(-1);
        assertThat(headers.getAsString(KafkaHeaders.KEY, null)).isEqualTo("foo");
        assertThat(headers.getAsString(KafkaHeaders.TOPIC, null)).isNull();
        assertThat(headers.getAsInteger(KafkaHeaders.TIMESTAMP, -1)).isEqualTo(-1);
        assertThat(headers.getAsInteger(KafkaHeaders.OFFSET, -1)).isEqualTo(-1);
        Iterable<Header> kh = (Iterable<Header>) headers.get(KafkaHeaders.KAFKA_HEADERS);
        assertThat(kh).isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCreationOfMessageWithKeyAndValue() {
        Message<String> message = Message.of("bar", Headers.of(KafkaHeaders.KEY, "foo"));
        Headers headers = message.getHeaders();
        assertThat(headers.getAsInteger(KafkaHeaders.PARTITION, -1)).isEqualTo(-1);
        assertThat(headers.getAsString(KafkaHeaders.KEY, null)).isEqualTo("foo");
        assertThat(headers.getAsString(KafkaHeaders.TOPIC, null)).isNull();
        assertThat(headers.getAsInteger(KafkaHeaders.TIMESTAMP, -1)).isEqualTo(-1);
        assertThat(headers.getAsInteger(KafkaHeaders.OFFSET, -1)).isEqualTo(-1);
        Iterable<Header> kh = (Iterable<Header>) headers.get(KafkaHeaders.KAFKA_HEADERS);
        assertThat(kh).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreationOfKafkaMessageFromKeyAndValueAndTopic() {
        KafkaMessage<String, String> message = KafkaMessage.of("topic", "foo", "bar");
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isEqualTo("foo");
        assertThat(message.getTopic()).isEqualTo("topic");
        assertThat(message.getKafkaHeaders().unwrap()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(-1);
        assertThat(message.getTimestamp()).isEqualTo(-1);

        Headers headers = message.getHeaders();
        assertThat(headers.getAsInteger(KafkaHeaders.PARTITION, -1)).isEqualTo(-1);
        assertThat(headers.getAsString(KafkaHeaders.KEY, null)).isEqualTo("foo");
        assertThat(headers.getAsString(KafkaHeaders.TOPIC, null)).isEqualTo("topic");
        assertThat(headers.getAsInteger(KafkaHeaders.TIMESTAMP, -1)).isEqualTo(-1);
        assertThat(headers.getAsInteger(KafkaHeaders.OFFSET, -1)).isEqualTo(-1);
        Iterable<Header> kh = (Iterable<Header>) headers.get(KafkaHeaders.KAFKA_HEADERS);
        assertThat(kh).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreationOfKafkaMessageWithEverything() {
        long timestamp = System.currentTimeMillis();
        KafkaMessage<String, String> message = KafkaMessage.of("topic", "foo", "bar", timestamp, 2);
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isEqualTo("foo");
        assertThat(message.getTopic()).isEqualTo("topic");
        assertThat(message.getKafkaHeaders().unwrap()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(2);
        assertThat(message.getTimestamp()).isEqualTo(timestamp);

        Headers headers = message.getHeaders();
        assertThat(headers.getAsInteger(KafkaHeaders.PARTITION, -1)).isEqualTo(2);
        assertThat(headers.getAsString(KafkaHeaders.KEY, null)).isEqualTo("foo");
        assertThat(headers.getAsString(KafkaHeaders.TOPIC, null)).isEqualTo("topic");
        assertThat(headers.getAsLong(KafkaHeaders.TIMESTAMP, -1)).isEqualTo(timestamp);
        assertThat(headers.getAsInteger(KafkaHeaders.OFFSET, -1)).isEqualTo(-1);
        Iterable<Header> kh = (Iterable<Header>) headers.get(KafkaHeaders.KAFKA_HEADERS);
        assertThat(kh).isEmpty();
    }

    @Test
    public void testCreationOfKafkaMessageWithEverythingButWithNullValues() {
        KafkaMessage<String, String> message = KafkaMessage.of(null, null, "bar", -1, -1);
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isNull();
        assertThat(message.getTopic()).isNull();
        assertThat(message.getKafkaHeaders().unwrap()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(-1);
        assertThat(message.getTimestamp()).isEqualTo(-1);
    }

    @Test
    public void testHeaders() {
        KafkaMessage<String, String> message = KafkaMessage.of("a", "b");
        KafkaMessage<String, String> message1 = message.withHeader("x-key", "key", StandardCharsets.UTF_8);
        KafkaMessage<String, String> message2 = message1.withHeader("x-key-2", "key-2".getBytes());

        assertThat(message).isNotEqualTo(message1).isNotEqualTo(message2);
        assertThat(message1).isNotEqualTo(message2);

        MessageHeaders headers = message2.getKafkaHeaders();
        assertThat(headers.getOneAsString("x-key")).hasValue("key");
        assertThat(headers.getOneAsBytes("x-key-2")).hasValue("key-2".getBytes());

        Iterable<Header> h1 = message1.getHeaders().get(KafkaHeaders.KAFKA_HEADERS, Collections.emptyList());
        Iterable<Header> h2 = message2.getHeaders().get(KafkaHeaders.KAFKA_HEADERS, Collections.emptyList());

        assertThat(h1).hasSize(1).allSatisfy(h -> {
            assertThat(h.key()).isEqualTo("x-key");
            assertThat(h.value()).isEqualTo("key".getBytes());
        });

        assertThat(h2).hasSize(2)
                .anySatisfy(h -> {
                    assertThat(h.key()).isEqualTo("x-key");
                    assertThat(h.value()).isEqualTo("key".getBytes());
                })
                .anySatisfy(h -> {
                    assertThat(h.key()).isEqualTo("x-key-2");
                    assertThat(h.value()).isEqualTo("key-2".getBytes());
                });
    }

}
