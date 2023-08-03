package org.eclipse.microprofile.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

public class CustomMessageAckNackWithMetadataTest {

    private final MyMetadata myMetadata = new MyMetadata("bar");

    @Test
    public void testCreationFromPayloadOnly() {
        Message<String> message = () -> "foo";
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).isEmpty();
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();
        assertThat(message.getAckWithMetadata()).isNotNull();
        assertThat(message.getNackWithMetadata()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.ack(Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause"), Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
    }

    @Test
    public void testCreationFromPayloadAndMetadataOnly() {
        Message<String> message = new Message<>() {
            @Override
            public String getPayload() {
                return "foo";
            }

            @Override
            public Metadata getMetadata() {
                return Metadata.of(myMetadata);
            }
        };
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).hasSize(1).containsExactly(myMetadata);
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();
        assertThat(message.getAckWithMetadata()).isNotNull();
        assertThat(message.getNackWithMetadata()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.ack(Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause"), Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();

        message = new Message<>() {
            @Override
            public String getPayload() {
                return "foo";
            }

            @Override
            public Metadata getMetadata() {
                return Metadata.empty();
            }
        };
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).isEmpty();
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();

    }

    @Test
    public void testCreationFromPayloadAndMetadataAsIterable() {
        List<Object> metadata = Arrays.asList(myMetadata, new AtomicInteger(2));
        Message<String> message = new Message<>() {
            @Override
            public String getPayload() {
                return "foo";
            }

            @Override
            public Metadata getMetadata() {
                return Metadata.from(metadata);
            }
        };
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).hasSize(2);
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();
        assertThat(message.getAckWithMetadata()).isNotNull();
        assertThat(message.getNackWithMetadata()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.ack(Metadata.from(metadata)).toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause"), Metadata.from(metadata)).toCompletableFuture().join()).isNull();

        assertThatThrownBy(() -> Message.of("x", (Iterable<Object>) null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCreationFromPayloadAndAck() {
        AtomicInteger count = new AtomicInteger(0);
        Message<String> message = new Message<>() {
            @Override
            public String getPayload() {
                return "foo";
            }

            @Override
            public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
                return this::ack;
            }

            @Override
            public CompletionStage<Void> ack(Metadata metadata) {
                count.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }

        };
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).hasSize(0);
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();
        assertThat(message.getAckWithMetadata()).isNotNull();
        assertThat(message.getNackWithMetadata()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.ack(Metadata.empty()).toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause"), Metadata.empty()).toCompletableFuture().join()).isNull();

        assertThat(count).hasValue(2);

    }

    @Test
    public void testCreationFromPayloadMetadataAndAck() {
        AtomicInteger count = new AtomicInteger(0);
        Message<String> message = new Message<>() {
            @Override
            public String getPayload() {
                return "foo";
            }

            @Override
            public Metadata getMetadata() {
                return Metadata.of(myMetadata);
            }

            @Override
            public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
                return this::ack;
            }

            @Override
            public CompletionStage<Void> ack(Metadata metadata) {
                count.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }

        };
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).hasSize(1).containsExactly(myMetadata);
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();
        assertThat(message.getAckWithMetadata()).isNotNull();
        assertThat(message.getNackWithMetadata()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.ack(Metadata.empty()).toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(count).hasValue(2);

        assertThat(Message.of("foo", null, () -> CompletableFuture.completedFuture(null)).getMetadata())
                .isEmpty();
    }

    @Test
    public void testCreationFromPayloadMetadataAsIterableAndAck() {
        List<Object> metadata = Arrays.asList(myMetadata, new AtomicInteger(2));
        AtomicInteger count = new AtomicInteger(0);
        Message<String> message = new Message<>() {
            @Override
            public String getPayload() {
                return "foo";
            }

            @Override
            public Metadata getMetadata() {
                return Metadata.from(metadata);
            }

            @Override
            public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
                return this::ack;
            }

            @Override
            public CompletionStage<Void> ack(Metadata metadata) {
                count.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }

        };

        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).hasSize(2).contains(myMetadata);
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();
        assertThat(message.getAckWithMetadata()).isNotNull();
        assertThat(message.getNackWithMetadata()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.ack(Metadata.from(metadata)).toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause"), Metadata.from(metadata)).toCompletableFuture().join()).isNull();
        assertThat(count).hasValue(2);

        assertThatThrownBy(() -> Message.of("foo", (Iterable<Object>) null, () -> CompletableFuture.completedFuture(null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCreationFromPayloadMetadataAckAndNack() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        Message<String> message = new CustomMessage<>("foo", Metadata.of(myMetadata), ack, nack);

        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).hasSize(1).containsExactly(myMetadata);
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.ack(Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause"), Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(ack).hasValue(2);
        assertThat(nack).hasValue(2);
    }

    @Test
    public void testWithPayload() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        Message<String> message = new CustomMessage<>("foo", Metadata.of(myMetadata), ack, nack);

        Message<String> created = message.withPayload("bar");
        assertThat(created.getPayload()).isEqualTo("bar");
        assertThat(created.getMetadata()).hasSize(1).containsExactly(myMetadata);
        assertThat(created.getAck()).isNotNull();
        assertThat(created.getNack()).isNotNull();
        assertThat(message.getAckWithMetadata()).isNotNull();
        assertThat(message.getNackWithMetadata()).isNotNull();

        assertThat(created.ack().toCompletableFuture().join()).isNull();
        assertThat(created.ack(Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause"), Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(ack).hasValue(2);
        assertThat(nack).hasValue(2);

    }

    @Test
    public void testWithMetadata() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        Message<String> message = new CustomMessage<>("foo", Metadata.of(myMetadata), ack, nack);
        MyMetadata mm = new MyMetadata("hello");
        Message<String> created = message.withMetadata(Metadata.of(mm));
        assertThat(created.getPayload()).isEqualTo("foo");
        assertThat(created.getMetadata()).hasSize(1).containsExactly(mm);
        assertThat(created.getAck()).isNotNull();
        assertThat(created.getNack()).isNotNull();
        assertThat(message.getAckWithMetadata()).isNotNull();
        assertThat(message.getNackWithMetadata()).isNotNull();

        assertThat(created.ack().toCompletableFuture().join()).isNull();
        assertThat(created.ack(Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause"), Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(ack).hasValue(2);
        assertThat(nack).hasValue(2);

    }

    @Test
    public void testWithAck() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger ack2 = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        Message<String> message = new CustomMessage<>("foo", Metadata.of(myMetadata), ack, nack);
        Message<String> created = message.withAck(() -> {
            ack2.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        assertThat(created.getPayload()).isEqualTo("foo");
        assertThat(created.getMetadata()).hasSize(1).containsExactly(myMetadata);
        assertThat(created.getAck()).isNotNull();
        assertThat(created.getNack()).isNotNull();
        assertThat(message.getAckWithMetadata()).isNotNull();
        assertThat(message.getNackWithMetadata()).isNotNull();

        assertThat(created.ack().toCompletableFuture().join()).isNull();
        assertThat(created.ack(Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause"), Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(ack2).hasValue(2);
        assertThat(ack).hasValue(0);
        assertThat(nack).hasValue(2);

    }

    @Test
    public void testWithNack() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        AtomicInteger nack2 = new AtomicInteger(0);
        Message<String> message = new CustomMessage<>("foo", Metadata.of(myMetadata), ack, nack);

        Message<String> created = message.withNack(t -> {
            assertThat(t).hasMessage("cause");
            nack2.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        assertThat(created.getPayload()).isEqualTo("foo");
        assertThat(created.getMetadata()).hasSize(1).containsExactly(myMetadata);
        assertThat(created.getAck()).isNotNull();
        assertThat(created.getNack()).isNotNull();
        assertThat(message.getAckWithMetadata()).isNotNull();
        assertThat(message.getNackWithMetadata()).isNotNull();

        assertThat(created.ack().toCompletableFuture().join()).isNull();
        assertThat(created.ack(Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause"), Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(ack).hasValue(2);
        assertThat(nack2).hasValue(2);
        assertThat(nack).hasValue(0);
    }

    @Test
    public void testAddMetadata() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        Message<String> message = new CustomMessage<>("foo", Metadata.of(myMetadata), ack, nack);
        Message<String> created = message.addMetadata(new AtomicInteger(2));
        assertThat(created.getPayload()).isEqualTo("foo");
        assertThat(created.getMetadata()).hasSize(2).contains(myMetadata);
        assertThat(created.getAck()).isNotNull();
        assertThat(created.getNack()).isNotNull();
        assertThat(message.getAckWithMetadata()).isNotNull();
        assertThat(message.getNackWithMetadata()).isNotNull();

        assertThat(created.ack().toCompletableFuture().join()).isNull();
        assertThat(created.ack(Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause"), Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(ack).hasValue(2);
        assertThat(nack).hasValue(2);
    }

    @Test
    public void testAckAndNackNull() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        Message<String> message = new CustomMessage<>("foo", Metadata.of(myMetadata), ack, nack);
        Message<String> created = message.withAck(null).withNack(null);
        assertThat(created.getPayload()).isEqualTo("foo");
        assertThat(created.getMetadata()).hasSize(1).contains(myMetadata);
        assertThat(created.getAck()).isNotNull();
        assertThat(created.getNack()).isNotNull();
        assertThat(message.getAckWithMetadata()).isNotNull();
        assertThat(message.getNackWithMetadata()).isNotNull();

        assertThat(created.ack().toCompletableFuture().join()).isNull();
        assertThat(created.ack(Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause"), Metadata.of(myMetadata)).toCompletableFuture().join()).isNull();
        assertThat(ack).hasValue(0);
        assertThat(nack).hasValue(0);
    }

    @Test
    public void testAccessingMetadata() {
        Message<String> message = Message.of("hello", Metadata.of(myMetadata)).addMetadata(new AtomicInteger(2));

        assertThat(message.getMetadata(MyMetadata.class))
                .hasValueSatisfying(m -> assertThat(m.getValue()).isEqualTo("bar"));
        assertThat(message.getMetadata(AtomicInteger.class)).hasValueSatisfying(m -> assertThat(m.get()).isEqualTo(2));
        assertThat(message.getMetadata(String.class)).isEmpty();
        assertThatThrownBy(() -> message.getMetadata(null)).isInstanceOf(IllegalArgumentException.class);
    }

    private static class MyMetadata {
        private final String value;

        public MyMetadata(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    private static class CustomMessage<T> implements Message<T> {

        T payload;
        Metadata metadata;
        AtomicInteger ack;
        AtomicInteger nack;

        public CustomMessage(T payload, Metadata metadata, AtomicInteger ack, AtomicInteger nack) {
            this.payload = payload;
            this.metadata = metadata;
            this.ack = ack;
            this.nack = nack;
        }

        @Override
        public T getPayload() {
            return payload;
        }

        @Override
        public Metadata getMetadata() {
            return metadata;
        }

        @Override
        public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
            return this::ack;
        }

        @Override
        public BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
            return this::nack;
        }

        @Override
        public CompletionStage<Void> ack(Metadata metadata) {
            ack.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
            assertThat(reason).hasMessage("cause");
            nack.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }
}
