package io.smallrye.reactive.messaging.kafka;

import java.nio.charset.Charset;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

public class OutgoingKafkaRecord<K, T> implements KafkaRecord<K, T> {

    private final T value;
    private final Supplier<CompletionStage<Void>> ack;
    private final Metadata metadata;
    private final OutgoingKafkaRecordMetadata<K> kafkaMetadata;

    public OutgoingKafkaRecord(String topic, K key, T value, Instant timestamp, int partition, Headers headers,
            Supplier<CompletionStage<Void>> ack) {
        kafkaMetadata = new OutgoingKafkaRecordMetadata<>(topic, key,
                partition, timestamp, headers);
        this.metadata = Metadata.of(kafkaMetadata);
        this.value = value;
        this.ack = ack;
    }

    @SuppressWarnings("unchecked")
    public static <K, T> OutgoingKafkaRecord<K, T> from(Message<T> message) {
        OutgoingKafkaRecordMetadata<K> kafkaMetadata = message
                .getMetadata(OutgoingKafkaRecordMetadata.class)
                .orElseThrow(() -> new IllegalArgumentException(
                        "`message` does not contain metadata of class " + OutgoingKafkaRecordMetadata.class));

        return new OutgoingKafkaRecord<>(kafkaMetadata.getTopic(), kafkaMetadata.getKey(), message.getPayload(),
                kafkaMetadata.getTimestamp(), kafkaMetadata.getPartition(), kafkaMetadata.getHeaders(), message.getAck());
    }

    @Override
    public CompletionStage<Void> ack() {
        if (ack == null) {
            return CompletableFuture.completedFuture(null);
        } else {
            return ack.get();
        }
    }

    @Override
    public T getPayload() {
        return this.value;
    }

    @Override
    public K getKey() {
        return kafkaMetadata.getKey();
    }

    @Override
    public String getTopic() {
        return kafkaMetadata.getTopic();
    }

    @Override
    public Instant getTimestamp() {
        return kafkaMetadata.getTimestamp();
    }

    @Override
    public Headers getHeaders() {
        return kafkaMetadata.getHeaders();
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return ack;
    }

    @Override
    public int getPartition() {
        return kafkaMetadata.getPartition();
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * Creates a new outgoing Kafka Message with a header added to the header list.
     *
     * @param key the header key
     * @param content the header key, must not be {@code null}
     * @return the updated Kafka Message.
     */
    public OutgoingKafkaRecord<K, T> withHeader(String key, byte[] content) {
        Headers headers = getHeaders();
        Headers copy = new RecordHeaders(headers);
        copy.add(new Header() {
            @Override
            public String key() {
                return key;
            }

            @Override
            public byte[] value() {
                return content;
            }
        });
        return new OutgoingKafkaRecord<>(getTopic(), getKey(), getPayload(), getTimestamp(), getPartition(),
                copy, getAck());
    }

    /**
     * Creates a new outgoing Kafka Message with a header added to the header list.
     *
     * @param key the header key
     * @param content the header key, must not be {@code null}
     * @return the updated Kafka Message.
     */
    public OutgoingKafkaRecord<K, T> withHeader(String key, String content) {
        Headers headers = getHeaders();
        Headers copy = new RecordHeaders(headers);
        copy.add(new Header() {
            @Override
            public String key() {
                return key;
            }

            @Override
            public byte[] value() {
                return content.getBytes();
            }
        });
        return new OutgoingKafkaRecord<>(getTopic(), getKey(), getPayload(), getTimestamp(), getPartition(),
                copy, getAck());
    }

    /**
     * Creates a new outgoing Kafka Message with a header added to the header list.
     *
     * @param key the header key
     * @param content the header key, must not be {@code null}
     * @param enc the encoding, must not be {@code null}
     * @return the updated Kafka Message.
     */
    public OutgoingKafkaRecord<K, T> withHeader(String key, String content, Charset enc) {
        Headers headers = getHeaders();
        Headers copy = new RecordHeaders(headers);
        copy.add(new Header() {
            @Override
            public String key() {
                return key;
            }

            @Override
            public byte[] value() {
                return content.getBytes(enc);
            }
        });
        return new OutgoingKafkaRecord<>(getTopic(), getKey(), getPayload(), getTimestamp(), getPartition(),
                copy, getAck());
    }

    @Override
    public <P> OutgoingKafkaRecord<K, P> withPayload(P payload) {
        return OutgoingKafkaRecord.from(Message.of(payload, getMetadata(), getAck()));
    }

    @Override
    public OutgoingKafkaRecord<K, T> withMetadata(Iterable<Object> metadata) {
        Metadata newMetadata = getMetadata().with(metadata);
        return OutgoingKafkaRecord.from(Message.of(getPayload(), newMetadata, getAck()));
    }

    @Override
    public OutgoingKafkaRecord<K, T> withMetadata(Metadata metadata) {
        Metadata newMetadata = getMetadata().with(metadata);
        return OutgoingKafkaRecord.from(Message.of(getPayload(), newMetadata, getAck()));
    }

    @Override
    public OutgoingKafkaRecord<K, T> withAck(Supplier<CompletionStage<Void>> supplier) {
        return OutgoingKafkaRecord.from(Message.of(getPayload(), getMetadata(), supplier));
    }
}
