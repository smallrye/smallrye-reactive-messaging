package io.smallrye.reactive.messaging.ft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.faulttolerance.Asynchronous;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.faulttolerance.FaultToleranceExtension;
import io.smallrye.metrics.MetricRegistries;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class RetryTest extends WeldTestBaseWithoutTails {

    @BeforeEach
    public void initFaultTolerance() {
        addExtensionClass(FaultToleranceExtension.class);
        addBeanClass(MetricRegistries.class);
    }

    @Test
    public void testRetryOnPayloadProcessor() {
        addBeanClass(MessageGenerator.class, FailingProcessorOfPayload.class);
        initialize();
        FailingProcessorOfPayload processor = get(FailingProcessorOfPayload.class);
        await().until(() -> processor.list().size() == 10);
        assertThat(processor.list()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        MessageGenerator generator = get(MessageGenerator.class);
        await().until(() -> generator.acks() == 10);
        assertThat(generator.acks()).isEqualTo(10);
        assertThat(generator.nacks()).isEqualTo(0);
    }

    @Test
    public void testRetryOnPayloadProcessorCS() {
        addBeanClass(MessageGenerator.class, FailingProcessorOfPayloadCS.class);
        initialize();
        FailingProcessorOfPayloadCS processor = get(FailingProcessorOfPayloadCS.class);
        await().until(() -> processor.list().size() == 10);
        assertThat(processor.list()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        MessageGenerator generator = get(MessageGenerator.class);
        await().until(() -> generator.acks() == 10);
        assertThat(generator.acks()).isEqualTo(10);
        assertThat(generator.nacks()).isEqualTo(0);
    }

    @Test
    public void testRetryOnMessageProcessor() {
        addBeanClass(MessageGenerator.class, FailingProcessorOfMessage.class);
        initialize();
        FailingProcessorOfMessage processor = get(FailingProcessorOfMessage.class);
        await().until(() -> processor.list().size() == 10);
        assertThat(processor.list()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        MessageGenerator generator = get(MessageGenerator.class);
        await().until(() -> generator.acks() == 10);
        assertThat(generator.acks()).isEqualTo(10);
        assertThat(generator.nacks()).isEqualTo(0);
    }

    @Test
    public void testRetryOnMessageProcessorCS() {
        addBeanClass(MessageGenerator.class, FailingProcessorOfMessageCS.class);
        initialize();
        FailingProcessorOfMessageCS processor = get(FailingProcessorOfMessageCS.class);
        await().until(() -> processor.list().size() == 10);
        assertThat(processor.list()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        MessageGenerator generator = get(MessageGenerator.class);
        await().until(() -> generator.acks() == 10);
        assertThat(generator.acks()).isEqualTo(10);
        assertThat(generator.nacks()).isEqualTo(0);
    }

    @Test
    public void testRetryOnPayloadSubscriber() {
        addBeanClass(MessageGenerator.class, FailingSubscriberOfPayload.class);
        initialize();
        FailingSubscriberOfPayload subscriber = get(FailingSubscriberOfPayload.class);
        await().until(() -> subscriber.list().size() == 10);
        assertThat(subscriber.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        MessageGenerator generator = get(MessageGenerator.class);
        await().until(() -> generator.acks() == 10);
        assertThat(generator.acks()).isEqualTo(10);
        assertThat(generator.nacks()).isEqualTo(0);
    }

    @Test
    public void testRetryOnPayloadSubscriberCS() {
        addBeanClass(MessageGenerator.class, FailingSubscriberOfPayloadCS.class);
        initialize();
        FailingSubscriberOfPayloadCS subscriber = get(FailingSubscriberOfPayloadCS.class);
        await().until(() -> subscriber.list().size() == 10);
        assertThat(subscriber.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        MessageGenerator generator = get(MessageGenerator.class);
        await().until(() -> generator.acks() == 10);
        assertThat(generator.acks()).isEqualTo(10);
        assertThat(generator.nacks()).isEqualTo(0);
    }

    @Test
    public void testRetryOnMessageSubscriber() {
        addBeanClass(MessageGenerator.class, FailingSubscriberOfMessage.class);
        initialize();
        FailingSubscriberOfMessage subscriber = get(FailingSubscriberOfMessage.class);
        await().until(() -> subscriber.list().size() == 10);
        assertThat(subscriber.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        MessageGenerator generator = get(MessageGenerator.class);
        await().until(() -> generator.acks() == 10);
        assertThat(generator.acks()).isEqualTo(10);
        assertThat(generator.nacks()).isEqualTo(0);
    }

    @Test
    public void testRetryOnMessageSubscriberCS() {
        addBeanClass(MessageGenerator.class, FailingSubscriberOfMessageCS.class);
        initialize();
        FailingSubscriberOfMessageCS subscriber = get(FailingSubscriberOfMessageCS.class);
        await().until(() -> subscriber.list().size() == 10);
        assertThat(subscriber.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        MessageGenerator generator = get(MessageGenerator.class);
        await().until(() -> generator.acks() == 10);
        assertThat(generator.acks()).isEqualTo(10);
        assertThat(generator.nacks()).isEqualTo(0);
    }

    @Test
    public void testImpossibleRecovery() {
        addBeanClass(MessageGenerator.class, FailingForeverProcessorOfPayload.class);
        initialize();
        FailingForeverProcessorOfPayload processor = get(FailingForeverProcessorOfPayload.class);
        await().untilAsserted(() -> assertThat(processor.list()).containsExactly(1, 2, 3, 5, 6, 7, 8, 9, 10));
        MessageGenerator generator = get(MessageGenerator.class);
        await().until(() -> generator.acks() == 9);
        assertThat(generator.acks()).isEqualTo(9);
        assertThat(generator.nacks()).isEqualTo(1);

    }

    @ApplicationScoped
    public static class MessageGenerator {
        int acks;
        int nacks;

        public int acks() {
            return acks;
        }

        public int nacks() {
            return nacks;
        }

        @Outgoing("numbers")
        public Multi<Message<Integer>> numbers() {
            return Multi.createFrom().range(0, 10)
                    .onItem().transform(i -> Message.of(i, () -> {
                        acks++;
                        return CompletableFuture.completedFuture(null);
                    }, t -> {
                        nacks++;
                        return CompletableFuture.completedFuture(null);
                    }));
        }
    }

    @ApplicationScoped
    public static class FailingProcessorOfPayload {

        int attempt = 0;
        List<Integer> list = new ArrayList<>();

        @Incoming("numbers")
        @Outgoing("retry-sink")
        @Retry // 3 is the default
        public int process(int i) {
            if (attempt < 3 && i == 3) {
                attempt++;
                throw new ArithmeticException("boom");
            }
            return i + 1;
        }

        @Incoming("retry-sink")
        public void consume(int l) {
            list.add(l);
        }

        public List<Integer> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class FailingProcessorOfMessage {

        int attempt = 0;
        List<Integer> list = new ArrayList<>();

        @Incoming("numbers")
        @Outgoing("retry-sink")
        @Retry // 3 is the default
        public Message<Integer> process(Message<Integer> i) {
            if (attempt < 3 && i.getPayload() == 3) {
                attempt++;
                throw new ArithmeticException("boom");
            }
            return i.withPayload(i.getPayload() + 1);
        }

        @Incoming("retry-sink")
        public void consume(int l) {
            list.add(l);
        }

        public List<Integer> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class FailingSubscriberOfPayload {

        int attempt = 0;
        List<Integer> list = new ArrayList<>();

        @Incoming("numbers")
        @Retry // 3 is the default
        public void consume(int i) {
            if (attempt < 3 && i == 3) {
                attempt++;
                throw new ArithmeticException("boom");
            }
            list.add(i);
        }

        public List<Integer> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class FailingSubscriberOfMessage {

        int attempt = 0;
        List<Integer> list = new ArrayList<>();

        @Incoming("numbers")
        @Retry // 3 is the default
        public CompletionStage<Void> consume(Message<Integer> i) {
            if (attempt < 3 && i.getPayload() == 3) {
                attempt++;
                throw new ArithmeticException("boom");
            }
            list.add(i.getPayload());
            return i.ack();
        }

        public List<Integer> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class FailingForeverProcessorOfPayload {

        List<Integer> list = new ArrayList<>();

        @Incoming("numbers")
        @Outgoing("retry-sink")
        @Retry // 3 is the default
        public int process(int i) {
            if (i == 3) {
                throw new ArithmeticException("boom");
            }
            return i + 1;
        }

        @Incoming("retry-sink")
        public void consume(int l) {
            list.add(l);
        }

        public List<Integer> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class FailingProcessorOfPayloadCS {

        int attempt = 0;
        List<Integer> list = new ArrayList<>();

        @Incoming("numbers")
        @Outgoing("retry-sink")
        @Retry // 3 is the default
        @Asynchronous
        public CompletionStage<Integer> process(int i) {

            return CompletableFuture.supplyAsync(() -> {
                if (attempt < 3 && i == 3) {
                    attempt++;
                    throw new ArithmeticException("boom");
                }
                return i + 1;
            });
        }

        @Incoming("retry-sink")
        public void consume(int l) {
            list.add(l);
        }

        public List<Integer> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class FailingProcessorOfMessageCS {

        int attempt = 0;
        List<Integer> list = new ArrayList<>();

        @Incoming("numbers")
        @Outgoing("retry-sink")
        @Retry // 3 is the default
        @Asynchronous
        public CompletionStage<Message<Integer>> process(Message<Integer> i) {
            return CompletableFuture.supplyAsync(() -> {
                if (attempt < 3 && i.getPayload() == 3) {
                    attempt++;
                    throw new ArithmeticException("boom");
                }
                return i.withPayload(i.getPayload() + 1);
            });
        }

        @Incoming("retry-sink")
        public void consume(int l) {
            list.add(l);
        }

        public List<Integer> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class FailingSubscriberOfPayloadCS {

        int attempt = 0;
        List<Integer> list = new ArrayList<>();

        @Incoming("numbers")
        @Retry // 3 is the default
        @Asynchronous
        public CompletionStage<Void> consume(int i) {
            return CompletableFuture.runAsync(() -> {
                if (attempt < 3 && i == 3) {
                    attempt++;
                    throw new ArithmeticException("boom");
                }
                list.add(i);
            });
        }

        public List<Integer> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class FailingSubscriberOfMessageCS {

        int attempt = 0;
        List<Integer> list = new ArrayList<>();

        @Incoming("numbers")
        @Retry // 3 is the default
        @Asynchronous
        public CompletionStage<Void> consume(Message<Integer> i) {
            return CompletableFuture.runAsync(() -> {
                if (attempt < 3 && i.getPayload() == 3) {
                    attempt++;
                    throw new ArithmeticException("boom");
                }
                list.add(i.getPayload());

            }).thenCompose(x -> i.ack());
        }

        public List<Integer> list() {
            return list;
        }
    }
}
