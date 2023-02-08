package org.eclipse.microprofile.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

public class MessageTest {

    private final MyMetadata myMetadata = new MyMetadata("bar");

    @Test
    public void testCreationFromPayloadOnly() {
        Message<String> message = Message.of("foo");
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).isEmpty();
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
    }

    @Test
    public void testCreationFromPayloadAndMetadataOnly() {

        Message<String> message = Message.of("foo", Metadata.of(myMetadata));
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).hasSize(1).containsExactly(myMetadata);
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();

        message = Message.of("foo", (Metadata) null);
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).isEmpty();
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();

    }

    @Test
    public void testCreationFromPayloadAndMetadataAsIterable() {
        List<Object> metadata = Arrays.asList(myMetadata, new AtomicInteger(2));
        Message<String> message = Message.of("foo", metadata);
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).hasSize(2);
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();

        assertThatThrownBy(() -> Message.of("x", (Iterable<Object>) null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCreationFromPayloadAndAck() {
        AtomicInteger count = new AtomicInteger(0);
        Message<String> message = Message.of("foo", () -> {
            count.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).hasSize(0);
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(count).hasValue(1);

    }

    @Test
    public void testCreationFromPayloadMetadataAndAck() {
        AtomicInteger count = new AtomicInteger(0);
        Message<String> message = Message.of("foo", Metadata.of(myMetadata), () -> {
            count.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).hasSize(1).containsExactly(myMetadata);
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(count).hasValue(1);

        assertThat(Message.of("foo", null, () -> CompletableFuture.completedFuture(null)).getMetadata())
                .isEmpty();
    }

    @Test
    public void testCreationFromPayloadMetadataAsIterableAndAck() {
        List<Object> metadata = Arrays.asList(myMetadata, new AtomicInteger(2));
        AtomicInteger count = new AtomicInteger(0);
        Message<String> message = Message.of("foo", metadata, () -> {
            count.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).hasSize(2).contains(myMetadata);
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(count).hasValue(1);

        assertThatThrownBy(() -> Message.of("foo", (Iterable<Object>) null, () -> CompletableFuture.completedFuture(null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCreationFromPayloadMetadataAsIterableAckAndNack() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        Message<String> message = Message.of("foo", Collections.singleton(myMetadata),
                () -> {
                    ack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                t -> {
                    assertThat(t).hasMessage("cause");
                    nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }

        );
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).hasSize(1).containsExactly(myMetadata);
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(ack).hasValue(1);
        assertThat(nack).hasValue(1);
    }

    @Test
    public void testCreationFromPayloadMetadataAckAndNack() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        Message<String> message = Message.of("foo", Metadata.of(myMetadata),
                () -> {
                    ack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                t -> {
                    assertThat(t).hasMessage("cause");
                    nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }

        );
        assertThat(message.getPayload()).isEqualTo("foo");
        assertThat(message.getMetadata()).hasSize(1).containsExactly(myMetadata);
        assertThat(message.getAck()).isNotNull();
        assertThat(message.getNack()).isNotNull();

        assertThat(message.ack().toCompletableFuture().join()).isNull();
        assertThat(message.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(ack).hasValue(1);
        assertThat(nack).hasValue(1);
    }

    @Test
    public void testWithPayload() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        Message<String> message = Message.of("foo", Metadata.of(myMetadata),
                () -> {
                    ack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                t -> {
                    assertThat(t).hasMessage("cause");
                    nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }

        );

        Message<String> created = message.withPayload("bar");
        assertThat(created.getPayload()).isEqualTo("bar");
        assertThat(created.getMetadata()).hasSize(1).containsExactly(myMetadata);
        assertThat(created.getAck()).isNotNull();
        assertThat(created.getNack()).isNotNull();

        assertThat(created.ack().toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(ack).hasValue(1);
        assertThat(nack).hasValue(1);

    }

    @Test
    public void testWithMetadata() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        Message<String> message = Message.of("foo", Metadata.of(myMetadata),
                () -> {
                    ack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                t -> {
                    assertThat(t).hasMessage("cause");
                    nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }

        );

        MyMetadata mm = new MyMetadata("hello");
        Message<String> created = message.withMetadata(Metadata.of(mm));
        assertThat(created.getPayload()).isEqualTo("foo");
        assertThat(created.getMetadata()).hasSize(1).containsExactly(mm);
        assertThat(created.getAck()).isNotNull();
        assertThat(created.getNack()).isNotNull();

        assertThat(created.ack().toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(ack).hasValue(1);
        assertThat(nack).hasValue(1);

    }

    @Test
    public void testWithMetadataAsIterable() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        Message<String> message = Message.of("foo", Metadata.of(myMetadata),
                () -> {
                    ack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                t -> {
                    assertThat(t).hasMessage("cause");
                    nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }

        );

        MyMetadata mm = new MyMetadata("hello");
        Message<String> created = message.withMetadata(Collections.singletonList(mm));
        assertThat(created.getPayload()).isEqualTo("foo");
        assertThat(created.getMetadata()).hasSize(1).containsExactly(mm);
        assertThat(created.getAck()).isNotNull();
        assertThat(created.getNack()).isNotNull();

        assertThat(created.ack().toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(ack).hasValue(1);
        assertThat(nack).hasValue(1);

    }

    @Test
    public void testWithAck() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger ack2 = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        Message<String> message = Message.of("foo", Metadata.of(myMetadata),
                () -> {
                    ack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                t -> {
                    assertThat(t).hasMessage("cause");
                    nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }

        );

        Message<String> created = message.withAck(() -> {
            ack2.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        assertThat(created.getPayload()).isEqualTo("foo");
        assertThat(created.getMetadata()).hasSize(1).containsExactly(myMetadata);
        assertThat(created.getAck()).isNotNull();
        assertThat(created.getNack()).isNotNull();

        assertThat(created.ack().toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(ack2).hasValue(1);
        assertThat(ack).hasValue(0);
        assertThat(nack).hasValue(1);

    }

    @Test
    public void testWithNack() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        AtomicInteger nack2 = new AtomicInteger(0);
        Message<String> message = Message.of("foo", Metadata.of(myMetadata),
                () -> {
                    ack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                t -> {
                    assertThat(t).hasMessage("cause");
                    nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }

        );

        Message<String> created = message.withNack(t -> {
            assertThat(t).hasMessage("cause");
            nack2.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        assertThat(created.getPayload()).isEqualTo("foo");
        assertThat(created.getMetadata()).hasSize(1).containsExactly(myMetadata);
        assertThat(created.getAck()).isNotNull();
        assertThat(created.getNack()).isNotNull();

        assertThat(created.ack().toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(ack).hasValue(1);
        assertThat(nack2).hasValue(1);
        assertThat(nack).hasValue(0);
    }

    @Test
    public void testAddMetadata() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        Message<String> message = Message.of("foo", Metadata.of(myMetadata),
                () -> {
                    ack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                t -> {
                    assertThat(t).hasMessage("cause");
                    nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }

        );

        Message<String> created = message.addMetadata(new AtomicInteger(2));
        assertThat(created.getPayload()).isEqualTo("foo");
        assertThat(created.getMetadata()).hasSize(2).contains(myMetadata);
        assertThat(created.getAck()).isNotNull();
        assertThat(created.getNack()).isNotNull();

        assertThat(created.ack().toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
        assertThat(ack).hasValue(1);
        assertThat(nack).hasValue(1);
    }

    @Test
    public void testAckAndNackNull() {
        AtomicInteger ack = new AtomicInteger(0);
        AtomicInteger nack = new AtomicInteger(0);
        Message<String> message = Message.of("foo", Metadata.of(myMetadata),
                () -> {
                    ack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                t -> {
                    assertThat(t).hasMessage("cause");
                    nack.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }

        );

        Message<String> created = message.withAck(null).withNack(null);
        assertThat(created.getPayload()).isEqualTo("foo");
        assertThat(created.getMetadata()).hasSize(1).contains(myMetadata);
        assertThat(created.getAck()).isNull();
        assertThat(created.getNack()).isNull();
        assertThat(created.ack().toCompletableFuture().join()).isNull();
        assertThat(created.nack(new Exception("cause")).toCompletableFuture().join()).isNull();
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
}
