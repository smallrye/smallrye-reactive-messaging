package org.eclipse.microprofile.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

public class AckNackChainTest {

    @Test
    public void testAckChain() {
        AtomicInteger m1Ack = new AtomicInteger();
        AtomicInteger m2Ack = new AtomicInteger();
        AtomicInteger m3Ack = new AtomicInteger();
        Message<String> m1 = Message.of("1", () -> {
            m1Ack.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });

        Message<String> m2 = Message.of("2", () -> m1.getAck().get().thenAccept(x -> m2Ack.incrementAndGet()));

        Message<String> m3 = Message.of("3", () -> m2.ack().thenAccept(x -> m3Ack.incrementAndGet()));

        CompletionStage<Void> acked = m3.ack();
        acked.toCompletableFuture().join();

        assertThat(m3Ack).hasValue(1);
        assertThat(m2Ack).hasValue(1);
        assertThat(m1Ack).hasValue(1);
    }

    @Test
    public void testAckChainWithCustomMessageImplementingAck() {
        AtomicInteger m1Ack = new AtomicInteger();
        AtomicInteger m2Ack = new AtomicInteger();
        AtomicInteger m3Ack = new AtomicInteger();
        Message<String> m1 = new Message<>() {
            @Override
            public String getPayload() {
                return "1";
            }

            @Override
            public Supplier<CompletionStage<Void>> getAck() {
                return this::ack;
            }

            @Override
            public CompletionStage<Void> ack() {
                m1Ack.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
        };

        Message<String> m2 = Message.of("2", () -> m1.getAck().get().thenAccept(x -> m2Ack.incrementAndGet()));

        Message<String> m3 = Message.of("3", () -> m2.ack().thenAccept(x -> m3Ack.incrementAndGet()));

        CompletionStage<Void> acked = m3.ack();
        acked.toCompletableFuture().join();

        assertThat(m3Ack).hasValue(1);
        assertThat(m2Ack).hasValue(1);
        assertThat(m1Ack).hasValue(1);
    }

    @Test
    public void testAckChainWithCustomMessageImplementingAckWithMetadata() {
        AtomicInteger m1Ack = new AtomicInteger();
        AtomicInteger m2Ack = new AtomicInteger();
        AtomicInteger m3Ack = new AtomicInteger();
        Message<String> m1 = new Message<>() {
            @Override
            public String getPayload() {
                return "1";
            }

            @Override
            public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
                return metadata -> {
                    m1Ack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                };
            }

        };

        Message<String> m2 = Message.of("2", () -> m1.getAck().get().thenAccept(x -> m2Ack.incrementAndGet()));

        Message<String> m3 = Message.of("3", () -> m2.ack().thenAccept(x -> m3Ack.incrementAndGet()));

        CompletionStage<Void> acked = m3.ack();
        acked.toCompletableFuture().join();

        assertThat(m3Ack).hasValue(1);
        assertThat(m2Ack).hasValue(1);
        assertThat(m1Ack).hasValue(1);
    }

    @Test
    public void testNackChain() {
        AtomicInteger m1Nack = new AtomicInteger();
        AtomicInteger m2Nack = new AtomicInteger();
        AtomicInteger m3Nack = new AtomicInteger();
        Message<String> m1 = Message.of("1", Metadata.empty(),
                () -> CompletableFuture.completedFuture(null),
                cause -> {
                    assertThat(cause).isNotNull();
                    m1Nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });

        Message<String> m2 = Message.of("2", Metadata.empty(),
                () -> CompletableFuture.completedFuture(null),
                cause -> {
                    assertThat(cause).isNotNull();
                    return m1.getNack().apply(cause).thenAccept(x -> m2Nack.incrementAndGet());
                });

        Message<String> m3 = Message.of("3", Metadata.empty(),
                () -> CompletableFuture.completedFuture(null),
                cause -> {
                    assertThat(cause).isNotNull();
                    return m2.nack(cause).thenAccept(x -> m3Nack.incrementAndGet());
                });

        CompletionStage<Void> nacked = m3.nack(new Exception("boom"));
        nacked.toCompletableFuture().join();

        assertThat(m3Nack).hasValue(1);
        assertThat(m2Nack).hasValue(1);
        assertThat(m1Nack).hasValue(1);
    }

    @Test
    public void testNackChainWithCustomMessage() {
        AtomicInteger m1Nack = new AtomicInteger();
        AtomicInteger m2Nack = new AtomicInteger();
        AtomicInteger m3Nack = new AtomicInteger();

        Message<String> m1 = new Message<>() {
            @Override
            public String getPayload() {
                return "1";
            }

            @Override
            public Supplier<CompletionStage<Void>> getAck() {
                return () -> CompletableFuture.completedFuture(null);
            }

            @Override
            public Function<Throwable, CompletionStage<Void>> getNack() {
                return cause -> {
                    assertThat(cause).isNotNull();
                    m1Nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                };
            }
        };

        Message<String> m2 = Message.of("2", Metadata.empty(),
                () -> CompletableFuture.completedFuture(null),
                cause -> {
                    assertThat(cause).isNotNull();
                    return m1.getNack().apply(cause).thenAccept(x -> m2Nack.incrementAndGet());
                });

        Message<String> m3 = Message.of("3", Metadata.empty(),
                () -> CompletableFuture.completedFuture(null),
                cause -> {
                    assertThat(cause).isNotNull();
                    return m2.nack(cause).thenAccept(x -> m3Nack.incrementAndGet());
                });

        CompletionStage<Void> nacked = m3.nack(new Exception("boom"));
        nacked.toCompletableFuture().join();

        assertThat(m3Nack).hasValue(1);
        assertThat(m2Nack).hasValue(1);
        assertThat(m1Nack).hasValue(1);
    }

    @Test
    public void testNackChainWithCustomMessageImplementingAckWithMetadata() {
        AtomicInteger m1Nack = new AtomicInteger();
        AtomicInteger m2Nack = new AtomicInteger();
        AtomicInteger m3Nack = new AtomicInteger();
        Message<String> m1 = new Message<>() {
            @Override
            public String getPayload() {
                return "1";
            }

            @Override
            public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
                return metadata -> CompletableFuture.completedFuture(null);
            }

            @Override
            public BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
                return (cause, metadata) -> {
                    assertThat(cause).isNotNull();
                    m1Nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                };
            }
        };

        Message<String> m2 = Message.of("2", Metadata.empty(),
                () -> CompletableFuture.completedFuture(null),
                cause -> {
                    assertThat(cause).isNotNull();
                    return m1.getNack().apply(cause).thenAccept(x -> m2Nack.incrementAndGet());
                });

        Message<String> m3 = Message.of("3", Metadata.empty(),
                () -> CompletableFuture.completedFuture(null),
                cause -> {
                    assertThat(cause).isNotNull();
                    return m2.nack(cause).thenAccept(x -> m3Nack.incrementAndGet());
                });

        CompletionStage<Void> nacked = m3.nack(new Exception("boom"));
        nacked.toCompletableFuture().join();

        assertThat(m3Nack).hasValue(1);
        assertThat(m2Nack).hasValue(1);
        assertThat(m1Nack).hasValue(1);
    }

}
