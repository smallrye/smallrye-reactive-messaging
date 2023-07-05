package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.junit.jupiter.api.Test;

import com.github.dockerjava.zerodep.shaded.org.apache.commons.codec.binary.Base64;

public class MessageMetadataTest {

    @Test
    void testOutgoingMessageWithKeyValueAndKeyValueSchema() {
        TypedMessageBuilderImpl<KeyValue<Integer, String>> messageBuilder = new TypedMessageBuilderImpl<>(null,
                Schema.KeyValue(Schema.INT32, Schema.STRING));

        TypedMessageBuilderImpl<KeyValue<Integer, String>> msg = (TypedMessageBuilderImpl<KeyValue<Integer, String>>) messageBuilder
                .value(new KeyValue<>(1, "value"));

        assertThat(msg.getMetadataBuilder().hasNullValue()).isFalse();
        assertThat(msg.getMetadataBuilder().hasNullPartitionKey()).isFalse();
        assertThat(msg.getMetadataBuilder().hasPartitionKey()).isFalse();
        assertThat(msg.hasKey()).isFalse();

        assertThatThrownBy(() -> msg.getKey()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testOutgoingMessageWithNullKeyValueAndKeyValueSchema() {
        TypedMessageBuilderImpl<?> messageBuilder = new TypedMessageBuilderImpl<>(null,
                Schema.KeyValue(Schema.STRING, Schema.STRING));

        TypedMessageBuilderImpl<?> msg = (TypedMessageBuilderImpl<?>) messageBuilder.key(null).value(null);

        assertThat(msg.getMetadataBuilder().hasNullValue()).isTrue();
        assertThat(msg.getMetadataBuilder().hasNullPartitionKey()).isTrue();
        assertThat(msg.getMetadataBuilder().hasPartitionKey()).isFalse();
        assertThat(msg.hasKey()).isFalse();

        assertThatThrownBy(() -> msg.getKey()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testOutgoingMessageWithNullKeyValueAndStringSchema() {
        TypedMessageBuilderImpl<?> messageBuilder = new TypedMessageBuilderImpl<>(null, Schema.STRING);

        TypedMessageBuilderImpl<?> msg = (TypedMessageBuilderImpl<?>) messageBuilder.value(null);

        assertThat(msg.getMetadataBuilder().hasNullValue()).isTrue();
        assertThat(msg.getMetadataBuilder().hasPartitionKey()).isFalse();
        assertThat(msg.hasKey()).isFalse();

        assertThatThrownBy(() -> msg.getKey()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testOutgoingMessageWithKeyAndStringSchema() {
        TypedMessageBuilderImpl<?> messageBuilder = new TypedMessageBuilderImpl<>(null, Schema.STRING);

        TypedMessageBuilderImpl<?> msg = (TypedMessageBuilderImpl<?>) messageBuilder.key("some-string-key");

        assertThat(msg.getMetadataBuilder().hasNullValue()).isFalse();
        assertThat(msg.getMetadataBuilder().hasPartitionKey()).isTrue();
        assertThat(msg.getMetadataBuilder().getPartitionKey()).isEqualTo("some-string-key");
        assertThat(msg.getMetadataBuilder().hasNullPartitionKey()).isFalse();
        assertThat(msg.hasKey()).isTrue();

    }

    @Test
    void testOutgoingMessageWithKeyBytesAndStringSchema() {
        TypedMessageBuilderImpl<?> messageBuilder = new TypedMessageBuilderImpl<>(null, Schema.STRING);

        String key = "some-string-key";
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        TypedMessageBuilderImpl<?> msg = (TypedMessageBuilderImpl<?>) messageBuilder.keyBytes(keyBytes);

        assertThat(msg.getMetadataBuilder().hasNullValue()).isFalse();
        assertThat(msg.getMetadataBuilder().hasPartitionKey()).isTrue();
        assertThat(msg.getMetadataBuilder().getPartitionKey()).isEqualTo(Base64.encodeBase64String(keyBytes));
        assertThat(msg.getMetadataBuilder().hasNullPartitionKey()).isFalse();
        assertThat(msg.hasKey()).isTrue();

    }
}
