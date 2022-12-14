package io.smallrye.reactive.messaging.blocking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.vertx.core.VertxOptions;

public class UnorderedBlockingTest extends WeldTestBaseWithoutTails {

    private static final int COUNT = 100;

    private static final List<String> LIST = Multi.createFrom().range(1, COUNT + 1).map(i -> Integer.toString(i))
            .collect().asList()
            .await().indefinitely();

    @Test
    public void testWithProcessorItemToItem() {
        addBeanClass(ProcessBean.class);
        initialize();
        ProcessBean collector = container.select(ProcessBean.class).get();
        await().until(() -> collector.captures().size() == COUNT);

        assertThat(collector.captures().stream().map(c -> c.result).collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(LIST);
        assertThat(collector.captures().stream().map(c -> c.thread).collect(Collectors.toSet()))
                .hasSizeGreaterThanOrEqualTo(1);
        // The default Vert.x worker thread pool has 20 threads
        assertThat(collector.max()).isEqualTo(VertxOptions.DEFAULT_WORKER_POOL_SIZE);
    }

    @Test
    public void testWithProcessorItemToMessage() {
        addBeanClass(ProcessToMessageBean.class);
        initialize();
        ProcessToMessageBean collector = container.select(ProcessToMessageBean.class).get();
        await().until(() -> collector.captures().size() == COUNT);

        assertThat(collector.captures().stream().map(c -> c.result).collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(LIST);
        assertThat(collector.captures().stream().map(c -> c.thread).collect(Collectors.toSet()))
                .hasSizeGreaterThanOrEqualTo(1);
        // The default Vert.x worker thread pool has 20 threads
        assertThat(collector.max()).isEqualTo(VertxOptions.DEFAULT_WORKER_POOL_SIZE);
    }

    @Test
    public void testWithPublisherOfPayload() {
        addBeanClass(PublisherBean.class);
        initialize();
        PublisherBean collector = container.select(PublisherBean.class).get();
        await().until(() -> collector.captures().size() == COUNT);

        assertThat(collector.captures().stream().map(c -> c.thread).collect(Collectors.toSet()))
                .hasSizeGreaterThanOrEqualTo(1);
        // The default Vert.x worker thread pool has 20 threads
        assertThat(collector.max()).isEqualTo(VertxOptions.DEFAULT_WORKER_POOL_SIZE);
    }

    @Test
    public void testWithPublisherOfMessage() {
        addBeanClass(MessagePublisherBean.class);
        initialize();
        MessagePublisherBean collector = container.select(MessagePublisherBean.class).get();
        await().until(() -> collector.captures().size() == COUNT);

        assertThat(collector.captures().stream().map(c -> c.thread).collect(Collectors.toSet()))
                .hasSizeGreaterThanOrEqualTo(1);
        // The default Vert.x worker thread pool has 20 threads
        assertThat(collector.max()).isEqualTo(VertxOptions.DEFAULT_WORKER_POOL_SIZE);
    }

    @Test
    public void testWithSubscriberReturningVoid() {
        addBeanClass(SubscriberReturningVoidBean.class);
        initialize();
        SubscriberReturningVoidBean collector = container.select(SubscriberReturningVoidBean.class).get();
        await().until(() -> collector.captures().size() == COUNT);

        assertThat(collector.captures().stream().map(c -> c.result).collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(LIST);
        assertThat(collector.captures().stream().map(c -> c.thread).collect(Collectors.toSet()))
                .hasSizeGreaterThanOrEqualTo(1);
        // The default Vert.x worker thread pool has 20 threads
        assertThat(collector.max()).isEqualTo(VertxOptions.DEFAULT_WORKER_POOL_SIZE);
    }

    @Test
    public void testWithSubscriberReturningUni() {
        addBeanClass(SubscriberReturningUniBean.class);
        initialize();
        SubscriberReturningUniBean collector = container.select(SubscriberReturningUniBean.class).get();
        await().until(() -> collector.captures().size() == COUNT);

        assertThat(collector.captures().stream().map(c -> c.result).collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(LIST);
        assertThat(collector.captures().stream().map(c -> c.thread).collect(Collectors.toSet()))
                .hasSizeGreaterThanOrEqualTo(1);
        // The default Vert.x worker thread pool has 20 threads
        assertThat(collector.max()).isEqualTo(VertxOptions.DEFAULT_WORKER_POOL_SIZE);
    }

    @Test
    public void testWithSubscriberReturningCS() {
        addBeanClass(SubscriberReturningCSBean.class);
        initialize();
        SubscriberReturningCSBean collector = container.select(SubscriberReturningCSBean.class).get();
        await().until(() -> collector.captures().size() == COUNT);

        assertThat(collector.captures().stream().map(c -> c.result).collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(LIST);
        assertThat(collector.captures().stream().map(c -> c.thread).collect(Collectors.toSet()))
                .hasSizeGreaterThanOrEqualTo(1);
        // The default Vert.x worker thread pool has 20 threads
        assertThat(collector.max()).isEqualTo(VertxOptions.DEFAULT_WORKER_POOL_SIZE);
    }

    @ApplicationScoped
    public static class ProcessBean {

        private final List<Capture> sink = new CopyOnWriteArrayList<>();
        private final Random random = new Random();

        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicInteger maxConcurrency = new AtomicInteger();

        @Outgoing("in")
        public Multi<String> source() {
            return Multi.createFrom().iterable(LIST);
        }

        @Incoming("in")
        @Outgoing("bar")
        @Blocking(ordered = false)
        public String process(String s) throws InterruptedException {
            int c = wip.incrementAndGet();
            Thread.sleep(random.nextInt(10)); // Introduce a random delay which will change the ordering of the items
            maxConcurrency.updateAndGet(i -> Math.max(i, c));
            wip.decrementAndGet();
            return s;
        }

        public int max() {
            return maxConcurrency.get();
        }

        @Incoming("bar")
        public void sink(String s) {
            sink.add(new Capture(s));
        }

        public List<Capture> captures() {
            return sink;
        }

    }

    @ApplicationScoped
    public static class ProcessToMessageBean {

        private final List<Capture> sink = new CopyOnWriteArrayList<>();
        private final Random random = new Random();

        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicInteger maxConcurrency = new AtomicInteger();

        @Outgoing("in")
        public Multi<String> source() {
            return Multi.createFrom().iterable(LIST);
        }

        @Incoming("in")
        @Outgoing("bar")
        @Blocking(ordered = false)
        public Message<String> process(String s) throws InterruptedException {
            int c = wip.incrementAndGet();
            Thread.sleep(random.nextInt(10)); // Introduce a random delay which will change the ordering of the items
            maxConcurrency.updateAndGet(i -> Math.max(i, c));
            wip.decrementAndGet();
            return Message.of(s);
        }

        public int max() {
            return maxConcurrency.get();
        }

        @Incoming("bar")
        public void sink(String s) {
            sink.add(new Capture(s));
        }

        public List<Capture> captures() {
            return sink;
        }

    }

    private static class Capture {
        final String result;
        final String thread;

        public Capture(String result) {
            this.result = result;
            this.thread = Thread.currentThread().getName();
        }
    }

    @ApplicationScoped
    public static class PublisherBean {

        private final List<Capture> sink = new CopyOnWriteArrayList<>();
        private final Random random = new Random();

        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicInteger maxConcurrency = new AtomicInteger();

        @Outgoing("in")
        @Blocking(ordered = false)
        public String generator() throws InterruptedException {
            int c = wip.incrementAndGet();
            Thread.sleep(random.nextInt(10)); // Introduce a random delay which will change the ordering of the items
            maxConcurrency.updateAndGet(i -> Math.max(i, c));
            wip.decrementAndGet();
            return "hello";
        }

        @Incoming("in")
        @Outgoing("bar")
        public Multi<String> process(Multi<String> s) {
            return s.select().first(100);
        }

        public int max() {
            return maxConcurrency.get();
        }

        @Incoming("bar")
        public void sink(String s) {
            sink.add(new Capture(s));
        }

        public List<Capture> captures() {
            return sink;
        }

    }

    @ApplicationScoped
    public static class MessagePublisherBean {

        private final List<Capture> sink = new CopyOnWriteArrayList<>();
        private final Random random = new Random();

        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicInteger maxConcurrency = new AtomicInteger();

        @Outgoing("in")
        @Blocking(ordered = false)
        public Message<String> generator() throws InterruptedException {
            int c = wip.incrementAndGet();
            Thread.sleep(random.nextInt(10)); // Introduce a random delay which will change the ordering of the items
            maxConcurrency.updateAndGet(i -> Math.max(i, c));
            wip.decrementAndGet();
            return Message.of("hello");
        }

        @Incoming("in")
        @Outgoing("bar")
        public Multi<String> process(Multi<String> s) {
            return s.select().first(100);
        }

        public int max() {
            return maxConcurrency.get();
        }

        @Incoming("bar")
        public void sink(String s) {
            sink.add(new Capture(s));
        }

        public List<Capture> captures() {
            return sink;
        }

    }

    @ApplicationScoped
    public static class SubscriberReturningVoidBean {

        private final List<Capture> sink = new CopyOnWriteArrayList<>();
        private final Random random = new Random();

        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicInteger maxConcurrency = new AtomicInteger();

        @Outgoing("in")
        public Multi<String> source() {
            return Multi.createFrom().iterable(LIST);
        }

        public int max() {
            return maxConcurrency.get();
        }

        @Incoming("in")
        @Blocking(ordered = false)
        public void sink(String s) throws InterruptedException {
            int c = wip.incrementAndGet();
            Thread.sleep(random.nextInt(10)); // Introduce a random delay which will change the ordering of the items
            maxConcurrency.updateAndGet(i -> Math.max(i, c));
            wip.decrementAndGet();
            sink.add(new Capture(s));
        }

        public List<Capture> captures() {
            return sink;
        }

    }

    @ApplicationScoped
    public static class SubscriberReturningUniBean {

        private final List<Capture> sink = new CopyOnWriteArrayList<>();
        private final Random random = new Random();

        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicInteger maxConcurrency = new AtomicInteger();

        @Outgoing("in")
        public Multi<String> source() {
            return Multi.createFrom().iterable(LIST);
        }

        public int max() {
            return maxConcurrency.get();
        }

        @Incoming("in")
        @Blocking(ordered = false)
        public Uni<Void> sink(String s) throws InterruptedException {
            int c = wip.incrementAndGet();
            Thread.sleep(random.nextInt(10)); // Introduce a random delay which will change the ordering of the items
            maxConcurrency.updateAndGet(i -> Math.max(i, c));
            wip.decrementAndGet();
            sink.add(new Capture(s));
            return Uni.createFrom().nullItem();
        }

        public List<Capture> captures() {
            return sink;
        }

    }

    @ApplicationScoped
    public static class SubscriberReturningCSBean {

        private final List<Capture> sink = new CopyOnWriteArrayList<>();
        private final Random random = new Random();

        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicInteger maxConcurrency = new AtomicInteger();

        @Outgoing("in")
        public Multi<String> source() {
            return Multi.createFrom().iterable(LIST);
        }

        public int max() {
            return maxConcurrency.get();
        }

        @Incoming("in")
        @Blocking(ordered = false)
        public CompletionStage<Void> sink(String s) throws InterruptedException {
            int c = wip.incrementAndGet();
            Thread.sleep(random.nextInt(10)); // Introduce a random delay which will change the ordering of the items
            maxConcurrency.updateAndGet(i -> Math.max(i, c));
            wip.decrementAndGet();
            sink.add(new Capture(s));
            return CompletableFuture.completedFuture(null);
        }

        public List<Capture> captures() {
            return sink;
        }

    }

}
