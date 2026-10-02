package io.smallrye.reactive.messaging.blocking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBase;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.vertx.core.Vertx;

public class BlockingBatchProcessorTest extends WeldTestBase {

    @Test
    public void testBlockingBatchProcessorWithSyncAck() {
        addBeanClass(SyncSource.class, BatchProcessor.class, SimpleSink.class);
        initialize();

        SimpleSink sink = container.select(SimpleSink.class).get();
        await().atMost(Duration.ofSeconds(10))
                .until(() -> sink.items().size() == 20);
        assertThat(sink.items()).containsExactlyElementsOf(
                Multi.createFrom().range(0, 20).map(i -> i + 1).map(String::valueOf)
                        .collect().asList().await().indefinitely());

        BatchProcessor processor = container.select(BatchProcessor.class).get();
        List<String> threads = processor.threads().stream().distinct().collect(Collectors.toList());
        for (String name : threads) {
            assertThat(name).startsWith("vert.x-worker-thread-");
        }
    }

    @Test
    public void testBlockingBatchProcessorWithAsyncAck() {
        addBeanClass(AsyncAckSource.class, BatchProcessor.class, SimpleSink.class);
        initialize();

        SimpleSink sink = container.select(SimpleSink.class).get();
        await().atMost(Duration.ofSeconds(10))
                .until(() -> sink.items().size() == 20);
        assertThat(sink.items()).hasSize(20);
    }

    @Test
    public void testBlockingBatchProcessorOrderingWithHighVolume() {
        addBeanClass(HighVolumeSource.class, BatchProcessor.class, SimpleSink.class);
        initialize();

        SimpleSink sink = container.select(SimpleSink.class).get();
        int count = 2000;
        await().atMost(Duration.ofSeconds(30))
                .until(() -> sink.items().size() == count);

        List<String> expected = Multi.createFrom().range(0, count)
                .map(i -> i + 1).map(String::valueOf)
                .collect().asList().await().indefinitely();
        assertThat(sink.items()).containsExactlyElementsOf(expected);

        BatchProcessor processor = container.select(BatchProcessor.class).get();
        List<String> threads = processor.threads().stream().distinct().collect(Collectors.toList());
        for (String name : threads) {
            assertThat(name).startsWith("vert.x-worker-thread-");
        }
    }

    @Test
    public void testBlockingBatchProcessorWithAsyncAckHighVolume() {
        addBeanClass(HighVolumeAsyncAckSource.class, BatchProcessor.class, SimpleSink.class);
        initialize();

        SimpleSink sink = container.select(SimpleSink.class).get();
        int count = 2000;
        await().atMost(Duration.ofSeconds(30))
                .until(() -> sink.items().size() == count);

        List<String> expected = Multi.createFrom().range(0, count)
                .map(i -> i + 1).map(String::valueOf)
                .collect().asList().await().indefinitely();
        assertThat(sink.items()).containsExactlyElementsOf(expected);
    }

    @ApplicationScoped
    public static class SyncSource {
        @Outgoing("in")
        public Flow.Publisher<Integer> source() {
            return Multi.createFrom().range(0, 20);
        }
    }

    @ApplicationScoped
    public static class AsyncAckSource {
        @Outgoing("in")
        public Flow.Publisher<Message<Integer>> source() {
            Vertx vertx = Vertx.vertx();
            return Multi.createFrom().range(0, 20)
                    .map(i -> Message.of(i, () -> {
                        CompletableFuture<Void> cf = new CompletableFuture<>();
                        vertx.runOnContext(v -> cf.complete(null));
                        return cf;
                    }));
        }
    }

    @ApplicationScoped
    public static class HighVolumeSource {
        @Outgoing("in")
        public Flow.Publisher<Integer> source() {
            return Multi.createFrom().range(0, 2000);
        }
    }

    @ApplicationScoped
    public static class HighVolumeAsyncAckSource {
        @Outgoing("in")
        public Flow.Publisher<Message<Integer>> source() {
            Vertx vertx = Vertx.vertx();
            return Multi.createFrom().range(0, 2000)
                    .map(i -> Message.of(i, () -> {
                        CompletableFuture<Void> cf = new CompletableFuture<>();
                        vertx.runOnContext(v -> cf.complete(null));
                        return cf;
                    }));
        }
    }

    @ApplicationScoped
    public static class BatchProcessor {
        private final List<String> threads = new CopyOnWriteArrayList<>();

        @Blocking
        @Incoming("in")
        @Outgoing("out")
        public String process(int value) {
            threads.add(Thread.currentThread().getName());
            return String.valueOf(value + 1);
        }

        public List<String> threads() {
            return threads;
        }
    }

    @ApplicationScoped
    public static class SimpleSink {
        private final List<String> items = new CopyOnWriteArrayList<>();
        private final AtomicBoolean completed = new AtomicBoolean();

        @Incoming("out")
        public CompletionStage<Void> consume(String item) {
            items.add(item);
            return CompletableFuture.completedFuture(null);
        }

        public List<String> items() {
            return items;
        }

        public boolean hasCompleted() {
            return completed.get();
        }
    }
}
