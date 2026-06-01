package io.smallrye.reactive.messaging.kafka.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;

class OrderedIncomingKafkaRecordTest {

    @Test
    void shouldRunPostProcessingWhenAckFails() {
        RuntimeException failure = new RuntimeException("ack failed");
        AtomicInteger postProcessing = new AtomicInteger();
        OrderedIncomingKafkaRecord<String, String> record = new OrderedIncomingKafkaRecord<>(
                record(Uni.createFrom().failure(failure), Uni.createFrom().voidItem()),
                postProcessing::incrementAndGet);

        CompletionException thrown = assertThrows(CompletionException.class,
                () -> record.ack(Metadata.empty()).toCompletableFuture().join());

        assertThat(thrown.getCause()).isSameAs(failure);
        assertThat(postProcessing).hasValue(1);
    }

    @Test
    void shouldPreserveAckFailureWhenPostProcessingFails() {
        RuntimeException failure = new RuntimeException("ack failed");
        RuntimeException postProcessingFailure = new RuntimeException("post-processing failed");
        OrderedIncomingKafkaRecord<String, String> record = new OrderedIncomingKafkaRecord<>(
                record(Uni.createFrom().failure(failure), Uni.createFrom().voidItem()),
                () -> {
                    throw postProcessingFailure;
                });

        CompletionException thrown = assertThrows(CompletionException.class,
                () -> record.ack(Metadata.empty()).toCompletableFuture().join());

        assertThat(thrown.getCause()).isSameAs(failure);
        assertThat(thrown.getCause().getSuppressed()).isEmpty();
    }

    @Test
    void shouldNotFailAckWhenOnlyPostProcessingFails() {
        OrderedIncomingKafkaRecord<String, String> record = new OrderedIncomingKafkaRecord<>(
                record(Uni.createFrom().voidItem(), Uni.createFrom().voidItem()),
                () -> {
                    throw new RuntimeException("post-processing failed");
                });

        assertDoesNotThrow(() -> record.ack(Metadata.empty()).toCompletableFuture().join());
    }

    @Test
    void shouldRunPostProcessingWhenNackFails() {
        RuntimeException failure = new RuntimeException("nack failed");
        AtomicInteger postProcessing = new AtomicInteger();
        OrderedIncomingKafkaRecord<String, String> record = new OrderedIncomingKafkaRecord<>(
                record(Uni.createFrom().voidItem(), Uni.createFrom().failure(failure)),
                postProcessing::incrementAndGet);

        CompletionException thrown = assertThrows(CompletionException.class,
                () -> record.nack(new RuntimeException("processing failed"), Metadata.empty())
                        .toCompletableFuture().join());

        assertThat(thrown.getCause()).isSameAs(failure);
        assertThat(postProcessing).hasValue(1);
    }

    @Test
    void shouldPreserveNackFailureWhenPostProcessingFails() {
        RuntimeException failure = new RuntimeException("nack failed");
        RuntimeException postProcessingFailure = new RuntimeException("post-processing failed");
        OrderedIncomingKafkaRecord<String, String> record = new OrderedIncomingKafkaRecord<>(
                record(Uni.createFrom().voidItem(), Uni.createFrom().failure(failure)),
                () -> {
                    throw postProcessingFailure;
                });

        CompletionException thrown = assertThrows(CompletionException.class,
                () -> record.nack(new RuntimeException("processing failed"), Metadata.empty())
                        .toCompletableFuture().join());

        assertThat(thrown.getCause()).isSameAs(failure);
        assertThat(thrown.getCause().getSuppressed()).isEmpty();
    }

    @Test
    void shouldNotFailNackWhenOnlyPostProcessingFails() {
        OrderedIncomingKafkaRecord<String, String> record = new OrderedIncomingKafkaRecord<>(
                record(Uni.createFrom().voidItem(), Uni.createFrom().voidItem()),
                () -> {
                    throw new RuntimeException("post-processing failed");
                });

        assertDoesNotThrow(() -> record.nack(new RuntimeException("processing failed"), Metadata.empty())
                .toCompletableFuture().join());
    }

    private IncomingKafkaRecord<String, String> record(Uni<Void> ackResult, Uni<Void> nackResult) {
        KafkaCommitHandler commitHandler = new KafkaCommitHandler() {
            @Override
            public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record) {
                return ackResult;
            }
        };
        KafkaFailureHandler failureHandler = new KafkaFailureHandler() {
            @Override
            public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record, Throwable reason, Metadata metadata) {
                return nackResult;
            }
        };
        return new IncomingKafkaRecord<>(
                new ConsumerRecord<>("ordered-topic", 0, 0, "key", "value"),
                "channel",
                0,
                commitHandler,
                failureHandler,
                false);
    }
}
