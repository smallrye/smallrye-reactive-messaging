package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.Test;

public class KafkaMessageTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testCreationOfKafkaMessageFromKeyAndValue() {
        KafkaMessage<String, String> message = KafkaMessage.of("foo", "bar");
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isEqualTo("foo");
        assertThat(message.getTopic()).isNull();
        assertThat(message.getHeaders().unwrap()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(-1);
        assertThat(message.getTimestamp()).isEqualTo(-1);
        assertThat(message.getOffset()).isEqualTo(-1);

        Metadata metadata = message.getMetadata();
        assertThat(metadata.getAsInteger(KafkaMetadata.OUTGOING_PARTITION, -1)).isEqualTo(-1);
        assertThat(metadata.getAsString(KafkaMetadata.OUTGOING_KEY, null)).isEqualTo("foo");
        assertThat(metadata.getAsString(KafkaMetadata.OUTGOING_TOPIC, null)).isNull();
        assertThat(metadata.getAsInteger(KafkaMetadata.OUTGOING_TIMESTAMP, -1)).isEqualTo(-1);
        Iterable<Header> kh = (Iterable<Header>) metadata.get(KafkaMetadata.OUTGOING_HEADERS);
        assertThat(kh).isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCreationOfMessageWithKeyAndValue() {
        Message<String> message = Message.of("bar", Metadata.of(KafkaMetadata.OUTGOING_KEY, "foo"));
        Metadata metadata = message.getMetadata();
        assertThat(metadata.getAsInteger(KafkaMetadata.OUTGOING_PARTITION, -1)).isEqualTo(-1);
        assertThat(metadata.getAsString(KafkaMetadata.OUTGOING_KEY, null)).isEqualTo("foo");
        assertThat(metadata.getAsString(KafkaMetadata.OUTGOING_TOPIC, null)).isNull();
        assertThat(metadata.getAsInteger(KafkaMetadata.OUTGOING_TIMESTAMP, -1)).isEqualTo(-1);
        Iterable<Header> kh = (Iterable<Header>) metadata.get(KafkaMetadata.OUTGOING_HEADERS);
        assertThat(kh).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreationOfKafkaMessageFromKeyAndValueAndTopic() {
        KafkaMessage<String, String> message = KafkaMessage.of("topic", "foo", "bar");
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isEqualTo("foo");
        assertThat(message.getTopic()).isEqualTo("topic");
        assertThat(message.getHeaders().unwrap()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(-1);
        assertThat(message.getTimestamp()).isEqualTo(-1);

        Metadata metadata = message.getMetadata();
        assertThat(metadata.getAsInteger(KafkaMetadata.OUTGOING_PARTITION, -1)).isEqualTo(-1);
        assertThat(metadata.getAsString(KafkaMetadata.OUTGOING_KEY, null)).isEqualTo("foo");
        assertThat(metadata.getAsString(KafkaMetadata.OUTGOING_TOPIC, null)).isEqualTo("topic");
        assertThat(metadata.getAsInteger(KafkaMetadata.OUTGOING_TIMESTAMP, -1)).isEqualTo(-1);
        Iterable<Header> kh = (Iterable<Header>) metadata.get(KafkaMetadata.OUTGOING_HEADERS);
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
        assertThat(message.getHeaders().unwrap()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(2);
        assertThat(message.getTimestamp()).isEqualTo(timestamp);

        Metadata metadata = message.getMetadata();
        assertThat(metadata.getAsInteger(KafkaMetadata.OUTGOING_PARTITION, -1)).isEqualTo(2);
        assertThat(metadata.getAsString(KafkaMetadata.OUTGOING_KEY, null)).isEqualTo("foo");
        assertThat(metadata.getAsString(KafkaMetadata.OUTGOING_TOPIC, null)).isEqualTo("topic");
        assertThat(metadata.getAsLong(KafkaMetadata.OUTGOING_TIMESTAMP, -1)).isEqualTo(timestamp);
        Iterable<Header> kh = (Iterable<Header>) metadata.get(KafkaMetadata.OUTGOING_HEADERS);
        assertThat(kh).isEmpty();
    }

    @Test
    public void testCreationOfKafkaMessageWithEverythingButWithNullValues() {
        KafkaMessage<String, String> message = KafkaMessage.of(null, null, "bar", -1, -1);
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isNull();
        assertThat(message.getTopic()).isNull();
        assertThat(message.getHeaders().unwrap()).isEmpty();
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

        MessageHeaders headers = message2.getHeaders();
        assertThat(headers.getOneAsString("x-key")).hasValue("key");
        assertThat(headers.getOneAsBytes("x-key-2")).hasValue("key-2".getBytes());

        Iterable<Header> h1 = message1.getMetadata().get(KafkaMetadata.OUTGOING_HEADERS, Collections.emptyList());
        Iterable<Header> h2 = message2.getMetadata().get(KafkaMetadata.OUTGOING_HEADERS, Collections.emptyList());

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
