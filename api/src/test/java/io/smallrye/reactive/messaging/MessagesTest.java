package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

public class MessagesTest {

    @Test
    public void testMergeOfTwoMessagesAndAck() {
        AtomicBoolean ackM1 = new AtomicBoolean();
        AtomicBoolean ackM2 = new AtomicBoolean();
        Message<String> m1 = Message.of("A").withAck(() -> {
            ackM1.set(true);
            return CompletableFuture.completedFuture(null);
        });
        Message<Integer> m2 = Message.of(1)
                .withAck(() -> {
                    ackM2.set(true);
                    return CompletableFuture.completedFuture(null);
                });

        Message<String> merged = Messages.merge(List.of(m1, m2), l -> l.get(0) + "-" + l.get(1));

        assertThat(merged.getPayload()).isEqualTo("A-1");

        assertThat(merged.ack()).isCompleted();
        assertThat(ackM1).isTrue();
        assertThat(ackM2).isTrue();
    }

    @Test
    public void testMergeOfTwoMessagesOfSameTypeAndAck() {
        AtomicBoolean ackM1 = new AtomicBoolean();
        AtomicBoolean ackM2 = new AtomicBoolean();
        Message<String> m1 = Message.of("A").withAck(() -> {
            ackM1.set(true);
            return CompletableFuture.completedFuture(null);
        });
        Message<String> m2 = Message.of("B")
                .withAck(() -> {
                    ackM2.set(true);
                    return CompletableFuture.completedFuture(null);
                });

        List<Message<?>> list = List.of(m1, m2);
        Message<String> merged = Messages.merge(list, l -> l.get(0) + "-" + l.get(1));

        assertThat(merged.getPayload()).isEqualTo("A-B");

        assertThat(merged.ack()).isCompleted();
        assertThat(ackM1).isTrue();
        assertThat(ackM2).isTrue();
    }

    @Test
    public void testMergeOfTwoMessagesAndNack() {
        AtomicBoolean nackM1 = new AtomicBoolean();
        AtomicBoolean nackM2 = new AtomicBoolean();
        Message<String> m1 = Message.of("A").withNack((e) -> {
            nackM1.set(true);
            return CompletableFuture.completedFuture(null);
        });
        Message<Integer> m2 = Message.of(1)
                .withNack((e) -> {
                    nackM2.set(true);
                    return CompletableFuture.completedFuture(null);
                });

        Message<String> merged = Messages.merge(List.of(m1, m2), l -> l.get(0) + "-" + l.get(1));

        assertThat(merged.getPayload()).isEqualTo("A-1");

        assertThat(merged.nack(new IOException("boom"))).isCompleted();
        assertThat(nackM1).isTrue();
        assertThat(nackM2).isTrue();
    }

    @Test
    public void testFailingCombination() {
        AtomicBoolean nackM1 = new AtomicBoolean();
        AtomicBoolean nackM2 = new AtomicBoolean();
        Message<String> m1 = Message.of("A").withNack((e) -> {
            nackM1.set(true);
            return CompletableFuture.completedFuture(null);
        });
        Message<Integer> m2 = Message.of(1)
                .withNack((e) -> {
                    nackM2.set(true);
                    return CompletableFuture.completedFuture(null);
                });

        assertThatThrownBy(() -> Messages.merge(List.of(m1, m2), l -> {
            throw new IllegalArgumentException("boom");
        })).isInstanceOf(IllegalArgumentException.class);

        assertThat(nackM1).isTrue();
        assertThat(nackM2).isTrue();
    }

    @Test
    void testMergeAsList() {
        AtomicBoolean ackM1 = new AtomicBoolean();
        AtomicBoolean ackM2 = new AtomicBoolean();
        AtomicBoolean ackM3 = new AtomicBoolean();
        Message<String> m1 = Message.of("A").withAck(() -> {
            ackM1.set(true);
            return CompletableFuture.completedFuture(null);
        });
        Message<String> m2 = Message.of("B")
                .withAck(() -> {
                    ackM2.set(true);
                    return CompletableFuture.completedFuture(null);
                });
        Message<String> m3 = Message.of("C")
                .withAck(() -> {
                    ackM3.set(true);
                    return CompletableFuture.completedFuture(null);
                });

        Message<List<String>> result = Messages.merge(List.of(m1, m2, m3));
        assertThat(result.getPayload()).containsExactly("A", "B", "C");

        assertThat(result.ack()).isCompleted();
        assertThat(ackM1).isTrue();
        assertThat(ackM2).isTrue();
        assertThat(ackM3).isTrue();
    }

    @Test
    void testMergeAsListAndNack() {
        AtomicBoolean nackM1 = new AtomicBoolean();
        AtomicBoolean nackM2 = new AtomicBoolean();
        AtomicBoolean nackM3 = new AtomicBoolean();
        Message<String> m1 = Message.of("A").withNack((e) -> {
            nackM1.set(true);
            return CompletableFuture.completedFuture(null);
        });
        Message<String> m2 = Message.of("B")
                .withNack((e) -> {
                    nackM2.set(true);
                    return CompletableFuture.completedFuture(null);
                });
        Message<String> m3 = Message.of("C")
                .withNack((e) -> {
                    nackM3.set(true);
                    return CompletableFuture.completedFuture(null);
                });

        Message<List<String>> result = Messages.merge(List.of(m1, m2, m3));
        assertThat(result.getPayload()).containsExactly("A", "B", "C");

        assertThat(result.nack(new IOException("boom"))).isCompleted();
        assertThat(nackM1).isTrue();
        assertThat(nackM2).isTrue();
        assertThat(nackM3).isTrue();
    }

    @Test
    void testMetadataMerge() {
        Message<String> message1 = Message.of("A")
                .addMetadata(new NonMergeableMetadata("test-1"))
                .addMetadata(new MergeableMetadata("test-2"));

        Message<String> message2 = Message.of("B")
                .addMetadata(new IgnoredMergeableMetadata("test-3"))
                .addMetadata(new MergeableMetadata("test-4"))
                .addMetadata(new MustBeRemovedMetadata("test-must-be-removed-if-merged"));
        ;

        Message<String> message3 = Message.of("C")
                .addMetadata(new NonMergeableMetadata("test-5"))
                .addMetadata(new AnotherMetadata("test-6"))
                .addMetadata(new MergeableMetadata("test-7"))
                .addMetadata(new IgnoredMergeableMetadata("test-8"));

        Message<List<String>> result = Messages.merge(List.of(message1, message2, message3));
        assertThat(result.getPayload()).containsExactly("A", "B", "C");
        assertThat(result.getMetadata(NonMergeableMetadata.class)).isPresent()
                .hasValueSatisfying(v -> assertThat(v.value).isEqualTo("test-1"));
        assertThat(result.getMetadata(MergeableMetadata.class)).isPresent()
                .hasValueSatisfying(v -> assertThat(v.value).isEqualTo("test-2|test-4|test-7"));
        assertThat(result.getMetadata(IgnoredMergeableMetadata.class)).isPresent()
                .hasValueSatisfying(v -> assertThat(v.value).isEqualTo("test-3"));
        assertThat(result.getMetadata(AnotherMetadata.class)).isPresent()
                .hasValueSatisfying(v -> assertThat(v.value).isEqualTo("test-6"));
        assertThat(result.getMetadata(MustBeRemovedMetadata.class)).isPresent()
                .hasValueSatisfying(v -> assertThat(v.value).isEqualTo("test-must-be-removed-if-merged"));

        message3 = message3.addMetadata(new MustBeRemovedMetadata("boom"));
        result = Messages.merge(List.of(message1, message2, message3));
        assertThat(result.getPayload()).containsExactly("A", "B", "C");
        assertThat(result.getMetadata(NonMergeableMetadata.class)).isPresent()
                .hasValueSatisfying(v -> assertThat(v.value).isEqualTo("test-1"));
        assertThat(result.getMetadata(MergeableMetadata.class)).isPresent()
                .hasValueSatisfying(v -> assertThat(v.value).isEqualTo("test-2|test-4|test-7"));
        assertThat(result.getMetadata(IgnoredMergeableMetadata.class)).isPresent()
                .hasValueSatisfying(v -> assertThat(v.value).isEqualTo("test-3"));
        assertThat(result.getMetadata(AnotherMetadata.class)).isPresent()
                .hasValueSatisfying(v -> assertThat(v.value).isEqualTo("test-6"));
        assertThat(result.getMetadata(MustBeRemovedMetadata.class)).isEmpty();

    }

    @Test
    void checkWithEmptyList() {
        assertThat(Messages.merge(List.of(), l -> l).getPayload()).isEqualTo(Collections.emptyList());
        assertThat(Messages.merge(List.of()).getPayload()).isEqualTo(Collections.emptyList());
    }

    @Test
    void checkSimpleChainAcknowledgement() {
        AtomicBoolean o1Ack = new AtomicBoolean();
        AtomicBoolean o2Ack = new AtomicBoolean();
        AtomicInteger i1Ack = new AtomicInteger();
        Message<String> o1 = Message.of("foo", () -> {
            o1Ack.set(true);
            return CompletableFuture.completedFuture(null);
        });
        Message<String> o2 = Message.of("bar", () -> {
            o2Ack.set(true);
            return CompletableFuture.completedFuture(null);
        });

        Message<Integer> i = Message.of(1, () -> {
            i1Ack.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });

        List<Message<?>> outcomes = Messages.chain(i).with(o1, o2);
        assertThat(i1Ack).hasValue(0);
        assertThat(o1Ack).isFalse();
        assertThat(o2Ack).isFalse();

        outcomes.get(0).ack();
        assertThat(i1Ack).hasValue(0);
        assertThat(o1Ack).isTrue();
        assertThat(o2Ack).isFalse();

        outcomes.get(1).ack();
        assertThat(i1Ack).hasValue(1);
        assertThat(o1Ack).isTrue();
        assertThat(o1Ack).isTrue();

        outcomes.get(1).ack();
        outcomes.get(0).ack();
        assertThat(i1Ack).hasValue(1);

        outcomes.get(1).nack(new Exception("boom"));
        outcomes.get(0).nack(new Exception("boom"));
        assertThat(i1Ack).hasValue(1);
    }

    @Test
    void checkSimpleChainNegativeAcknowledgement() {
        AtomicBoolean o1Ack = new AtomicBoolean();
        AtomicBoolean o2Ack = new AtomicBoolean();
        AtomicBoolean o1Nack = new AtomicBoolean();
        AtomicBoolean o2Nack = new AtomicBoolean();
        AtomicInteger i1Ack = new AtomicInteger();
        AtomicInteger i1Nack = new AtomicInteger();

        Message<String> o1 = Message.of("foo", () -> {
            o1Ack.set(true);
            return CompletableFuture.completedFuture(null);
        }, t -> {
            o1Nack.set(true);
            return CompletableFuture.completedFuture(null);
        });
        Message<String> o2 = Message.of("bar", () -> {
            o2Ack.set(true);
            return CompletableFuture.completedFuture(null);
        }, t -> {
            o2Nack.set(true);
            return CompletableFuture.completedFuture(null);
        });

        Message<Integer> i = Message.of(1, () -> {
            i1Ack.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }, t -> {
            i1Nack.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });

        List<Message<?>> outcomes = Messages.chain(i).with(o1, o2);
        assertThat(i1Ack).hasValue(0);
        assertThat(o1Ack).isFalse();
        assertThat(o2Ack).isFalse();
        assertThat(i1Nack).hasValue(0);
        assertThat(o1Nack).isFalse();
        assertThat(o2Nack).isFalse();

        outcomes.get(0).ack();
        assertThat(i1Ack).hasValue(0);
        assertThat(o1Ack).isTrue();
        assertThat(o2Ack).isFalse();
        assertThat(i1Nack).hasValue(0);
        assertThat(o1Nack).isFalse();
        assertThat(o2Nack).isFalse();

        outcomes.get(0).nack(new Exception("boom"));
        assertThat(i1Ack).hasValue(0);
        assertThat(i1Nack).hasValue(0);

        outcomes.get(1).nack(new Exception("boom"));
        assertThat(i1Nack).hasValue(1);
        assertThat(i1Ack).hasValue(0);
        assertThat(o2Nack).isTrue();

        outcomes.get(1).ack();
        assertThat(i1Nack).hasValue(1);
        assertThat(i1Ack).hasValue(0);
    }

    @Test
    void testChainWithMetadataSelection() {
        Message<Integer> i = Message.of(1)
                .withMetadata(List.of(new NonMergeableMetadata("hello"), new MergeableMetadata("hello"),
                        new AnotherMetadata("hello")));

        Message<String> m1 = Message.of("a");
        AnotherMetadata am = new AnotherMetadata("hello");
        Message<String> m2 = Message.of("b").addMetadata(am);

        // No metadata copied from the original message
        List<Message<?>> out = Messages.chain(i).withoutMetadata().with(m1, m2);
        assertThat(out.get(0).getMetadata()).isEmpty();
        assertThat(out.get(1).getMetadata()).hasSize(1).containsOnly(am);

        // All metadata are copied from the original message
        out = Messages.chain(i).with(m1, m2);
        assertThat(out.get(0).getMetadata()).hasSize(3);
        assertThat(out.get(1).getMetadata()).hasSize(3).doesNotContain(am);

        // All metadata but MergeableMetadata are copied from the original message
        out = Messages.chain(i).withoutMetadata(MergeableMetadata.class).with(m1, m2);
        assertThat(out.get(0).getMetadata()).hasSize(2);
        assertThat(out.get(1).getMetadata()).hasSize(2).doesNotContain(am);

        // All metadata but AnotherMetadata are copied from the original message
        out = Messages.chain(i).withoutMetadata(AnotherMetadata.class).with(m1, m2);
        assertThat(out.get(0).getMetadata()).hasSize(2);
        assertThat(out.get(1).getMetadata()).hasSize(3).contains(am);

        out = Messages.chain(i).withoutMetadata().withMetadata(AnotherMetadata.class).with(m1, m2);
        assertThat(out.get(0).getMetadata()).hasSize(1);
        assertThat(out.get(1).getMetadata()).hasSize(1);

    }

    public static class NonMergeableMetadata {
        String value;

        public NonMergeableMetadata(String v) {
            this.value = v;
        }
    }

    public static class MergeableMetadata implements io.smallrye.reactive.messaging.MergeableMetadata<MergeableMetadata> {
        String value;

        public MergeableMetadata(String value) {
            this.value = value;
        }

        @Override
        public MergeableMetadata merge(MergeableMetadata other) {
            return new MergeableMetadata(this.value + "|" + other.value);
        }

        @Override
        public String toString() {
            return "MergeableMetadata{" +
                    "value='" + value + '\'' +
                    '}';
        }
    }

    public static class IgnoredMergeableMetadata
            implements io.smallrye.reactive.messaging.MergeableMetadata<IgnoredMergeableMetadata> {
        String value;

        public IgnoredMergeableMetadata(String value) {
            this.value = value;
        }

        @Override
        public IgnoredMergeableMetadata merge(IgnoredMergeableMetadata other) {
            return this;
        }
    }

    public static class AnotherMetadata {
        String value;

        public AnotherMetadata(String value) {
            this.value = value;
        }
    }

    public static class MustBeRemovedMetadata
            implements io.smallrye.reactive.messaging.MergeableMetadata<MustBeRemovedMetadata> {
        String value;

        public MustBeRemovedMetadata(String value) {
            this.value = value;
        }

        @Override
        public MustBeRemovedMetadata merge(MustBeRemovedMetadata other) {
            return null;
        }
    }

}
