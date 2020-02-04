package io.smallrye.reactive.messaging.kafka;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaRecordTest {

    @Test
    public void testCreationOfKafkaRecordFromKeyAndValue() {
        Message<String> message = Message.<String>newBuilder().payload("bar").metadata(OutgoingKafkaRecordMetadata.<String>builder().withKey("foo").build()).build();
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getKey()).isEqualTo("foo");
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getTopic()).isNull();
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getHeaders()).isEmpty();
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getPartition()).isEqualTo(-1);
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getTimestamp()).isEqualTo(-1);

        OutgoingKafkaRecordMetadata<?> metadata = message.getMetadata(OutgoingKafkaRecordMetadata.class)
            .orElseThrow(() -> new AssertionError("Metadata expected"));
        assertThat(metadata.getPartition()).isEqualTo(-1);
        assertThat(metadata.getKey()).isEqualTo("foo");
        assertThat(metadata.getTopic()).isNull();
        assertThat(metadata.getTimestamp()).isEqualTo(-1);
        assertThat(metadata.getHeaders()).isEmpty();
    }

    @Test
    public void testCreationOfMessageWithKeyAndValue() {
        Message<String> message = Message.<String>newBuilder().payload("bar").metadata(Metadata.of(OutgoingKafkaRecordMetadata.builder().withKey("foo").build())).build();
        OutgoingKafkaRecordMetadata<?> metadata = message.getMetadata(OutgoingKafkaRecordMetadata.class)
            .orElseThrow(() -> new AssertionError("Metadata expected"));
        assertThat(metadata.getPartition()).isEqualTo(-1);
        assertThat(metadata.getKey()).isEqualTo("foo");
        assertThat(metadata.getTopic()).isNull();
        assertThat(metadata.getTimestamp()).isEqualTo(-1);
        assertThat(metadata.getHeaders()).isEmpty();
    }

    @Test
    public void testCreationOfKafkaRecordFromKeyAndValueAndTopic() {
        Message<String> message = Message.<String>newBuilder().payload("bar").metadata(OutgoingKafkaRecordMetadata.<String>builder().withTopic("topic").withKey("foo").build()).build();
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getKey()).isEqualTo("foo");
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getTopic()).isEqualTo("topic");
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getHeaders()).isEmpty();
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getPartition()).isEqualTo(-1);
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getTimestamp()).isEqualTo(-1);

        OutgoingKafkaRecordMetadata<?> metadata = message.getMetadata(OutgoingKafkaRecordMetadata.class)
            .orElseThrow(() -> new AssertionError("Metadata expected"));
        assertThat(metadata.getPartition()).isEqualTo(-1);
        assertThat(metadata.getKey()).isEqualTo("foo");
        assertThat(metadata.getTopic()).isEqualTo("topic");
        assertThat(metadata.getTimestamp()).isEqualTo(-1);
        assertThat(metadata.getHeaders()).isEmpty();
    }

    @Test
    public void testCreationOfKafkaRecordWithEverything() {
        long timestamp = System.currentTimeMillis();
        Message<String> message = Message.<String>newBuilder().payload("bar").metadata(
            OutgoingKafkaRecordMetadata.<String>builder().withTopic("topic").withKey("foo")
                .withTimestamp(timestamp).withPartition(2).build()).build();
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getKey()).isEqualTo("foo");
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getTopic()).isEqualTo("topic");
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getHeaders()).isEmpty();
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getPartition()).isEqualTo(2);
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getTimestamp()).isEqualTo(timestamp);

        OutgoingKafkaRecordMetadata<?> metadata = message.getMetadata(OutgoingKafkaRecordMetadata.class)
            .orElseThrow(() -> new AssertionError("Metadata expected"));
        assertThat(metadata.getPartition()).isEqualTo(2);
        assertThat(metadata.getKey()).isEqualTo("foo");
        assertThat(metadata.getTopic()).isEqualTo("topic");
        assertThat(metadata.getTimestamp()).isEqualTo(timestamp);
        assertThat(metadata.getHeaders()).isEmpty();
    }

    @Test
    public void testCreationOfKafkaRecordWithEverythingButWithNullValues() {
        Message<String> message = Message.<String>newBuilder().payload("bar").metadata(
            OutgoingKafkaRecordMetadata.<String>builder().withTopic(null).withKey(null)
                .withTimestamp(-1).withPartition(-1).build()).build();
        assertThat(message.getPayload()).isEqualTo("bar");
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getKey()).isNull();
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getTopic()).isNull();
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getHeaders()).isEmpty();
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getPartition()).isEqualTo(-1);
        assertThat(message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getTimestamp()).isEqualTo(-1);
    }

    @Test
    public void testHeaders() { // TODO
        Message<String> message = Message.<String>newBuilder().payload("b").metadata(OutgoingKafkaRecordMetadata.<String>builder().withKey("a").build()).build();

        Headers headers = message.getMetadata(OutgoingKafkaRecordMetadata.class).get().getHeaders();

        Headers copy1 = new RecordHeaders(headers);
        copy1.add(new RecordHeader("x-key", "key".getBytes(StandardCharsets.UTF_8)));
        Message<String> message1 = Message.<String>newBuilder().of(message).metadata(OutgoingKafkaRecordMetadata.builder().withHeaders(copy1).build()).build();

        Headers copy2 = new RecordHeaders(copy1);
        copy2.add(new RecordHeader("x-key-2", "key-2".getBytes()));
        Message<String> message2 = Message.<String>newBuilder().of(message).metadata(OutgoingKafkaRecordMetadata.builder().withHeaders(copy2).build()).build();

        assertThat(message).isNotEqualTo(message1).isNotEqualTo(message2);
        assertThat(message1).isNotEqualTo(message2);

        Headers headers2 = message2.getMetadata(OutgoingKafkaRecordMetadata.class).get().getHeaders();
        assertThat(new String(headers2.lastHeader("x-key").value())).isEqualTo("key");
        assertThat(headers2.lastHeader("x-key-2").value()).isEqualTo("key-2".getBytes());
    }

}
