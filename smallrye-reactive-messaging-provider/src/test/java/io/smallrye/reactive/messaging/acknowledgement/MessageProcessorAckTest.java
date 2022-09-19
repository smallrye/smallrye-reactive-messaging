package io.smallrye.reactive.messaging.acknowledgement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.vertx.core.impl.ConcurrentHashSet;

public class MessageProcessorAckTest extends WeldTestBaseWithoutTails {

    @BeforeEach
    public void deployBase() {
        addBeanClass(EmitterBean.class, Sink.class);
    }

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulProcessingOfMesssage() throws InterruptedException {
        addBeanClass(SuccessfulMessageProcessor.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulProcessingOfPayload() throws InterruptedException {
        addBeanClass(SuccessfulPayloadToMessageProcessor.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreNackedAfterFailingProcessingOfMessage() throws InterruptedException {
        addBeanClass(FailingMessageProcessor.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        List<Throwable> throwables = run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 9);
        assertThat(acked).hasSize(9);
        assertThat(nacked).hasSize(1);
        assertThat(throwables).hasSize(1)
                .allSatisfy(t -> assertThat(t).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("b"));
    }

    @Test
    public void testThatMessagesAreNackedAfterFailingProcessingOfPayload() throws InterruptedException {
        addBeanClass(FailingPayloadToMessageProcessor.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 9);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulBlockingProcessingOfMessage() throws InterruptedException {
        addBeanClass(SuccessfulBlockingMessageProcessor.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulBlockingProcessingOfPayload() throws InterruptedException {
        addBeanClass(SuccessfulBlockingPayloadToMessageProcessor.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreNackedAfterFailingBlockingProcessingOfMessage() throws InterruptedException {
        addBeanClass(FailingBlockingMessageProcessor.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        List<Throwable> throwables = run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 9);
        assertThat(acked).hasSize(9);
        assertThat(nacked).hasSize(1);
        assertThat(throwables).hasSize(1)
                .allSatisfy(t -> assertThat(t).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("b"));
    }

    @Test
    public void testThatMessagesAreNackedAfterFailingBlockingProcessingOfPayload() throws InterruptedException {
        addBeanClass(FailingBlockingPayloadToMessageProcessor.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 9);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    private List<Throwable> run(Set<String> acked, Set<String> nacked, Emitter<String> emitter)
            throws InterruptedException {
        List<Throwable> reasons = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);
        Multi.createFrom().items("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
                .onItem()
                .transformToUniAndMerge(i -> Uni.createFrom()
                        .completionStage(CompletableFuture.runAsync(() -> emitter.send(Message.of(i, Metadata.empty(),
                                () -> {
                                    acked.add(i);
                                    return CompletableFuture.completedFuture(null);
                                }, t -> {
                                    reasons.add(t);
                                    nacked.add(i);
                                    return CompletableFuture.completedFuture(null);
                                })))
                                .thenApply(x -> i)))
                .subscribe().with(
                        x -> {
                            // noop
                        },
                        f -> {
                            // noop
                        },
                        done::countDown);

        done.await(10, TimeUnit.SECONDS);
        return reasons;
    }

    @ApplicationScoped
    public static class Sink {
        List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("out")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class SuccessfulMessageProcessor {

        @Incoming("data")
        @Outgoing("out")
        public Message<String> process(Message<String> s) {
            return s.withPayload(s.getPayload().toUpperCase());
        }

    }

    @ApplicationScoped
    public static class SuccessfulPayloadToMessageProcessor {

        @Incoming("data")
        @Outgoing("out")
        @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
        public Message<String> process(String s) {
            return Message.of(s.toUpperCase());
        }

    }

    @ApplicationScoped
    public static class FailingMessageProcessor {

        @Incoming("data")
        @Outgoing("out")
        public Message<String> process(Message<String> m) {
            String s = m.getPayload();
            if (s.equalsIgnoreCase("b")) {
                // we cannot fail, we must nack explicitly.
                m.nack(new IllegalArgumentException("b")).toCompletableFuture().join();
                return null;
            }

            return m.withPayload(s.toUpperCase());
        }

    }

    @ApplicationScoped
    public static class FailingPayloadToMessageProcessor {

        @Incoming("data")
        @Outgoing("out")
        @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
        public Message<String> process(String s) {
            // NOTE: we cannot fail, as the message won't be nacked.
            if (s.equalsIgnoreCase("b")) {
                // skip
                return null;
            }

            return Message.of(s.toUpperCase());
        }

    }

    @ApplicationScoped
    public static class SuccessfulBlockingMessageProcessor {

        @Incoming("data")
        @Outgoing("out")
        @Blocking
        public Message<String> process(Message<String> s) {
            return CompletableFuture.supplyAsync(() -> s.withPayload(s.getPayload().toUpperCase())).join();
        }

    }

    @ApplicationScoped
    public static class SuccessfulBlockingPayloadToMessageProcessor {

        @Incoming("data")
        @Outgoing("out")
        @Blocking
        @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
        public Message<String> process(String s) {
            return CompletableFuture.supplyAsync(() -> Message.of(s.toUpperCase())).join();
        }

    }

    @ApplicationScoped
    public static class FailingBlockingMessageProcessor {

        @Incoming("data")
        @Outgoing("out")
        @Blocking
        public Message<String> process(Message<String> m) {
            String s = m.getPayload();
            return CompletableFuture.supplyAsync(() -> {
                if (s.equalsIgnoreCase("b")) {
                    // we cannot fail, we must nack explicitly.
                    m.nack(new IllegalArgumentException("b")).toCompletableFuture().join();
                    return null;
                }

                return m.withPayload(s.toUpperCase());
            }).join();
        }

    }

    @ApplicationScoped
    public static class FailingBlockingPayloadToMessageProcessor {

        @Incoming("data")
        @Outgoing("out")
        @Blocking
        @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
        public Message<String> process(String s) {
            return CompletableFuture.supplyAsync(() -> {
                if (s.equalsIgnoreCase("b")) {
                    return null;
                }

                return Message.of(s.toUpperCase());
            }).join();
        }

    }
}
