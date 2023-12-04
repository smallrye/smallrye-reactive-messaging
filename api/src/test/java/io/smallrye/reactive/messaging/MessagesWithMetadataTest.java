package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

public class MessagesWithMetadataTest {

    @Test
    public void testMergeOfTwoMessagesAndAckWithMetadata() {
        AtomicBoolean ackM1 = new AtomicBoolean();
        AtomicBoolean ackM2 = new AtomicBoolean();
        Class<Integer> metaType = Integer.class;
        Message<String> m1 = Message.of("A").withAckWithMetadata(metadata -> {
            if (metadata.get(metaType).isPresent()) {
                ackM1.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });
        Message<Integer> m2 = Message.of(1).withAckWithMetadata(metadata -> {
            if (metadata.get(metaType).isPresent()) {
                ackM2.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });

        Message<String> merged = Messages.merge(List.of(m1, m2), l -> l.get(0) + "-" + l.get(1));

        assertThat(merged.getPayload()).isEqualTo("A-1");

        assertThat(merged.ack(Metadata.of(1))).isCompleted();
        assertThat(ackM1).isTrue();
        assertThat(ackM2).isTrue();
    }

    @Test
    public void testMergeOfTwoMessagesOfSameTypeAndAckWithMetadata() {
        AtomicBoolean ackM1 = new AtomicBoolean();
        AtomicBoolean ackM2 = new AtomicBoolean();
        Message<String> m1 = Message.of("A").withAckWithMetadata(metadata -> {
            if (metadata.get(Integer.class).isPresent()) {
                ackM1.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });
        Message<String> m2 = Message.of("B").withAckWithMetadata(metadata -> {
            if (metadata.get(Integer.class).isPresent()) {
                ackM2.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });

        Message<String> merged = Messages.merge(List.of(m1, m2), l -> l.get(0) + "-" + l.get(1));

        assertThat(merged.getPayload()).isEqualTo("A-B");

        assertThat(merged.addMetadata(1).ack()).isCompleted();
        assertThat(ackM1).isTrue();
        assertThat(ackM2).isTrue();
    }

    @Test
    public void testMergeOfTwoMessagesAndNackWithMetadata() {
        AtomicBoolean nackM1 = new AtomicBoolean();
        AtomicBoolean nackM2 = new AtomicBoolean();
        Message<String> m1 = Message.of("A").withNackWithMetadata((throwable, metadata) -> {
            if (metadata.get(Integer.class).isPresent()) {
                nackM1.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });
        Message<Integer> m2 = Message.of(1).withNackWithMetadata((throwable, metadata) -> {
            if (metadata.get(Integer.class).isPresent()) {
                nackM2.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });

        Message<String> merged = Messages.merge(List.of(m1, m2), l -> l.get(0) + "-" + l.get(1));

        assertThat(merged.getPayload()).isEqualTo("A-1");

        assertThat(merged.nack(new IOException("boom"), Metadata.of(1))).isCompleted();
        assertThat(nackM1).isTrue();
        assertThat(nackM2).isTrue();
    }

    @Test
    void testMergeAsListWithMetadata() {
        AtomicBoolean ackM1 = new AtomicBoolean();
        AtomicBoolean ackM2 = new AtomicBoolean();
        AtomicBoolean ackM3 = new AtomicBoolean();
        Message<String> m1 = Message.of("A").withAckWithMetadata(metadata -> {
            if (metadata.get(Integer.class).isPresent()) {
                ackM1.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });
        Message<String> m2 = Message.of("B").withAckWithMetadata(metadata -> {
            if (metadata.get(Integer.class).isPresent()) {
                ackM2.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });
        Message<String> m3 = Message.of("C").withAckWithMetadata(metadata -> {
            if (metadata.get(Integer.class).isPresent()) {
                ackM3.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });

        Message<List<String>> result = Messages.merge(List.of(m1, m2, m3));
        assertThat(result.getPayload()).containsExactly("A", "B", "C");

        assertThat(result.ack(Metadata.of(1))).isCompleted();
        assertThat(ackM1).isTrue();
        assertThat(ackM2).isTrue();
        assertThat(ackM3).isTrue();
    }

    @Test
    void testMergeAsListAndNackWithMetadata() {
        AtomicBoolean nackM1 = new AtomicBoolean();
        AtomicBoolean nackM2 = new AtomicBoolean();
        AtomicBoolean nackM3 = new AtomicBoolean();
        Message<String> m1 = Message.of("A").withNackWithMetadata((throwable, metadata) -> {
            if (metadata.get(Integer.class).isPresent()) {
                nackM1.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });
        Message<String> m2 = Message.of("B").withNackWithMetadata((throwable, metadata) -> {
            if (metadata.get(Integer.class).isPresent()) {
                nackM2.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });
        Message<String> m3 = Message.of("C").withNackWithMetadata((throwable, metadata) -> {
            if (metadata.get(Integer.class).isPresent()) {
                nackM3.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });

        Message<List<String>> result = Messages.merge(List.of(m1, m2, m3));
        assertThat(result.getPayload()).containsExactly("A", "B", "C");

        assertThat(result.nack(new IOException("boom"), Metadata.of(1))).isCompleted();
        assertThat(nackM1).isTrue();
        assertThat(nackM2).isTrue();
        assertThat(nackM3).isTrue();
    }

    @Test
    void checkSimpleChainAcknowledgementWithMetadata() {
        AtomicBoolean o1Ack = new AtomicBoolean();
        AtomicBoolean o2Ack = new AtomicBoolean();
        AtomicInteger i1Ack = new AtomicInteger();
        Message<String> o1 = Message.of("foo", metadata -> {
            if (metadata.get(Integer.class).isPresent()) {
                o1Ack.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });
        Message<String> o2 = Message.of("bar", metadata -> {
            if (metadata.get(Integer.class).isPresent()) {
                o2Ack.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });

        Message<Integer> i = Message.of(1, metadata -> {
            if (metadata.get(Integer.class).isPresent()) {
                i1Ack.incrementAndGet();
            }
            return CompletableFuture.completedFuture(null);
        });

        List<Message<?>> outcomes = Messages.chain(i).with(o1, o2);
        assertThat(i1Ack).hasValue(0);
        assertThat(o1Ack).isFalse();
        assertThat(o2Ack).isFalse();

        outcomes.get(0).ack(Metadata.of(1));
        assertThat(i1Ack).hasValue(0);
        assertThat(o1Ack).isTrue();
        assertThat(o2Ack).isFalse();

        outcomes.get(1).ack(Metadata.of(1));
        assertThat(i1Ack).hasValue(1);
        assertThat(o1Ack).isTrue();
        assertThat(o1Ack).isTrue();

        outcomes.get(1).ack(Metadata.of(2));
        outcomes.get(0).ack(Metadata.of(2));
        assertThat(i1Ack).hasValue(1);

        outcomes.get(1).nack(new Exception("boom"));
        outcomes.get(0).nack(new Exception("boom"));
        assertThat(i1Ack).hasValue(1);
    }

    @Test
    void checkSimpleChainNegativeAcknowledgementWithMetadata() {
        AtomicBoolean o1Ack = new AtomicBoolean();
        AtomicBoolean o2Ack = new AtomicBoolean();
        AtomicBoolean o1Nack = new AtomicBoolean();
        AtomicBoolean o2Nack = new AtomicBoolean();
        AtomicInteger i1Ack = new AtomicInteger();
        AtomicInteger i1Nack = new AtomicInteger();
        Message<String> o1 = Message.of("foo", metadata -> {
            if (metadata.get(Integer.class).isPresent()) {
                o1Ack.set(true);
            }
            return CompletableFuture.completedFuture(null);
        }, (throwable, metadata) -> {
            if (metadata.get(Integer.class).isPresent()) {
                o1Nack.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });
        Message<String> o2 = Message.of("bar", metadata -> {
            if (metadata.get(Integer.class).isPresent()) {
                o2Ack.set(true);
            }
            return CompletableFuture.completedFuture(null);
        }, (throwable, metadata) -> {
            if (metadata.get(Integer.class).isPresent()) {
                o2Nack.set(true);
            }
            return CompletableFuture.completedFuture(null);
        });

        Message<Integer> i = Message.of(1, metadata -> {
            if (metadata.get(Integer.class).isPresent()) {
                i1Ack.incrementAndGet();
            }
            return CompletableFuture.completedFuture(null);
        }, (throwable, metadata) -> {
            if (metadata.get(Integer.class).isPresent()) {
                i1Nack.incrementAndGet();
            }
            return CompletableFuture.completedFuture(null);
        });

        List<Message<?>> outcomes = Messages.chain(i).with(o1, o2);
        assertThat(i1Ack).hasValue(0);
        assertThat(o1Ack).isFalse();
        assertThat(o2Ack).isFalse();
        assertThat(i1Nack).hasValue(0);
        assertThat(o1Nack).isFalse();
        assertThat(o2Nack).isFalse();

        outcomes.get(0).ack(Metadata.of(1));
        assertThat(i1Ack).hasValue(0);
        assertThat(o1Ack).isTrue();
        assertThat(o2Ack).isFalse();
        assertThat(i1Nack).hasValue(0);
        assertThat(o1Nack).isFalse();
        assertThat(o2Nack).isFalse();

        outcomes.get(0).nack(new Exception("boom"), Metadata.of(1));
        assertThat(i1Ack).hasValue(0);
        assertThat(i1Nack).hasValue(0);

        outcomes.get(1).nack(new Exception("boom"), Metadata.of(1));
        assertThat(i1Nack).hasValue(1);
        assertThat(i1Ack).hasValue(0);
        assertThat(o2Nack).isTrue();

        outcomes.get(1).ack(Metadata.of(1));
        assertThat(i1Nack).hasValue(1);
        assertThat(i1Ack).hasValue(0);
    }

}
