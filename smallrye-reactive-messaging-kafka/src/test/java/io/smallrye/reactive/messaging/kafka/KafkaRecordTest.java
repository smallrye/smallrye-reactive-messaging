package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.api.KafkaMetadataUtil;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

public class KafkaRecordTest {

    @Test
    public void testCreationOfKafkaRecordFromKeyAndValue() {
        KafkaRecord<String, String> message = KafkaRecord.of("foo", "bar");
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isEqualTo("foo");
        assertThat(message.getTopic()).isNull();
        assertThat(message.getHeaders()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(-1);
        assertThat(message.getTimestamp()).isNull();

        OutgoingKafkaRecordMetadata<?> metadata = message
                .getMetadata(OutgoingKafkaRecordMetadata.class)
                .orElseThrow(() -> new AssertionError("Metadata expected"));
        assertThat(metadata.getPartition()).isEqualTo(-1);
        assertThat(metadata.getKey()).isEqualTo("foo");
        assertThat(metadata.getTopic()).isNull();
        assertThat(metadata.getTimestamp()).isNull();
        assertThat(metadata.getHeaders()).isEmpty();
        LegacyMetadataTestUtils.tempCompareLegacyAndApiMetadata(metadata, message);
    }

    @Test
    public void testCreationOfMessageWithKeyAndValue() {
        Message<String> message = Message.of("bar", Metadata.of(OutgoingKafkaRecordMetadata.builder().withKey("foo").build()));
        OutgoingKafkaRecordMetadata<?> metadata = message.getMetadata(OutgoingKafkaRecordMetadata.class)
                .orElseThrow(() -> new AssertionError("Metadata expected"));
        assertThat(metadata.getPartition()).isEqualTo(-1);
        assertThat(metadata.getKey()).isEqualTo("foo");
        assertThat(metadata.getTopic()).isNull();
        assertThat(metadata.getTimestamp()).isNull();
        assertThat(metadata.getHeaders()).isNull();
    }

    @Test
    public void testCreationOfMessageAndAttachMetadataViaAPI() {
        Message<String> message = Message.of("bar");
        message = KafkaMetadataUtil.writeOutgoingKafkaMetadata(message,
                OutgoingKafkaRecordMetadata.<String> builder().withKey("boo").build());
        OutgoingKafkaRecordMetadata<?> metadata = message.getMetadata(OutgoingKafkaRecordMetadata.class)
                .orElseThrow(() -> new AssertionError("Metadata expected"));
        assertThat(metadata.getPartition()).isEqualTo(-1);
        assertThat(metadata.getKey()).isEqualTo("boo");
        assertThat(metadata.getTopic()).isNull();
        assertThat(metadata.getTimestamp()).isNull();
        assertThat(metadata.getHeaders()).isNull();
    }

    @Test
    public void testCreationOfKafkaRecordFromKeyAndValueAndTopic() {
        KafkaRecord<String, String> message = KafkaRecord.of("topic", "foo", "bar");
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isEqualTo("foo");
        assertThat(message.getTopic()).isEqualTo("topic");
        assertThat(message.getHeaders()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(-1);
        assertThat(message.getTimestamp()).isNull();

        OutgoingKafkaRecordMetadata<?> metadata = message.getMetadata(OutgoingKafkaRecordMetadata.class)
                .orElseThrow(() -> new AssertionError("Metadata expected"));
        assertThat(metadata.getPartition()).isEqualTo(-1);
        assertThat(metadata.getKey()).isEqualTo("foo");
        assertThat(metadata.getTopic()).isEqualTo("topic");
        assertThat(metadata.getTimestamp()).isNull();
        assertThat(metadata.getHeaders()).isEmpty();
        LegacyMetadataTestUtils.tempCompareLegacyAndApiMetadata(metadata, message);
    }

    @Test
    public void testCreationOfKafkaRecordWithEverything() {
        Instant timestamp = Instant.now();
        KafkaRecord<String, String> message = KafkaRecord.of("topic", "foo", "bar", timestamp, 2);
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isEqualTo("foo");
        assertThat(message.getTopic()).isEqualTo("topic");
        assertThat(message.getHeaders()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(2);
        assertThat(message.getTimestamp()).isEqualTo(timestamp);

        OutgoingKafkaRecordMetadata<?> metadata = message.getMetadata(OutgoingKafkaRecordMetadata.class)
                .orElseThrow(() -> new AssertionError("Metadata expected"));
        assertThat(metadata.getPartition()).isEqualTo(2);
        assertThat(metadata.getKey()).isEqualTo("foo");
        assertThat(metadata.getTopic()).isEqualTo("topic");
        assertThat(metadata.getTimestamp()).isEqualTo(timestamp);
        assertThat(metadata.getHeaders()).isEmpty();
        LegacyMetadataTestUtils.tempCompareLegacyAndApiMetadata(metadata, message);
    }

    @Test
    public void testCreationOfKafkaRecordWithEverythingButWithNullValues() {
        KafkaRecord<String, String> message = KafkaRecord.of(null, null, "bar", null, -1);
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getKey()).isNull();
        assertThat(message.getTopic()).isNull();
        assertThat(message.getHeaders()).isEmpty();
        assertThat(message.getPartition()).isEqualTo(-1);
        assertThat(message.getTimestamp()).isNull();
    }

    @Test
    public void testHeaders() {
        OutgoingKafkaRecord<String, String> message = KafkaRecord.of("a", "b");
        OutgoingKafkaRecord<String, String> message1 = message.withHeader("x-key", "key", StandardCharsets.UTF_8);
        OutgoingKafkaRecord<String, String> message2 = message1.withHeader("x-key-2", "key-2".getBytes());

        assertThat(message).isNotEqualTo(message1).isNotEqualTo(message2);
        assertThat(message1).isNotEqualTo(message2);

        Headers headers = message2.getHeaders();
        assertThat(new String(headers.lastHeader("x-key").value())).isEqualTo("key");
        assertThat(headers.lastHeader("x-key-2").value()).isEqualTo("key-2".getBytes());
    }

}
