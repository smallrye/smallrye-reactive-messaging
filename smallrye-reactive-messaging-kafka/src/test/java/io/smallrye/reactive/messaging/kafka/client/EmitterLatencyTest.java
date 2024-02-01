package io.smallrye.reactive.messaging.kafka.client;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.smallrye.reactive.messaging.kafka.TestTags;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionProxyTestBase;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@Tag(TestTags.SLOW)
public class EmitterLatencyTest extends KafkaCompanionProxyTestBase {

    @AfterEach
    void tearDown() throws IOException {
        toxics().getAll().stream().filter(toxic -> toxic.getName().equals("latency"))
                .findFirst()
                .ifPresent(toxic -> {
                    try {
                        toxic.remove();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    public void testDefaultEmitter() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        MapBasedConfig config = kafkaConfig("mp.messaging.outgoing.out")
                .with("topic", topic)
                .with("value.serializer", IntegerSerializer.class.getName());

        EmitterSender app = runApplication(config, EmitterSender.class);

        // here not creating the topic upfront causes too much wait
        companion.topics().create(topic, 5);

        toxics().latency("latency", ToxicDirection.UPSTREAM, 5000);

        List<CompletableFuture<Void>> sendAcks = new CopyOnWriteArrayList<>();

        for (int i = 0; i < 10000; i++) {
            int j = i;
            CompletableFuture<Void> future = new CompletableFuture<>();
            sendAcks.add(future);
            // KafkaLogging.log.warn("record :" + j);
            app.send(Message.of(j, () -> {
                // KafkaLogging.log.warn("sent " + j);
                future.complete(null);
                return future;
            }));
        }

        CompletableFuture.allOf(sendAcks.toArray(CompletableFuture[]::new))
                .get(2, TimeUnit.MINUTES);

        toxics().get("latency").remove();

        companion.consumeIntegers().fromTopics(topic, 10000)
                .awaitCompletion(Duration.of(2, ChronoUnit.MINUTES));
    }

    @Test
    public void testEmitterWithBuffer()
            throws IOException, ExecutionException, InterruptedException, TimeoutException {
        MapBasedConfig config = kafkaConfig("mp.messaging.outgoing.out")
                .with("topic", topic)
                .with("value.serializer", IntegerSerializer.class.getName());

        BufferedEmitterSender app = runApplication(config, BufferedEmitterSender.class);

        // don't create the topic upfront
        // companion.topics().create(topic, 5);

        toxics().latency("latency", ToxicDirection.UPSTREAM, 5000);

        List<CompletableFuture<Void>> sendAcks = new CopyOnWriteArrayList<>();

        for (int i = 0; i < 10000; i++) {
            int j = i;
            CompletableFuture<Void> future = new CompletableFuture<>();
            sendAcks.add(future);
            // KafkaLogging.log.warn("record :" + j);
            app.send(Message.of(j, () -> {
                // KafkaLogging.log.warn("sent " + j);
                future.complete(null);
                return future;
            }));
        }

        CompletableFuture.allOf(sendAcks.toArray(CompletableFuture[]::new))
                .get(2, TimeUnit.MINUTES);

        toxics().get("latency").remove();

        companion.consumeIntegers().fromTopics(topic, 10000)
                .awaitCompletion();
    }

    @Test
    public void testEmitterWithZeroBuffer()
            throws IOException, ExecutionException, InterruptedException, TimeoutException {
        MapBasedConfig config = kafkaConfig("mp.messaging.outgoing.out")
                .with("topic", topic)
                .with("value.serializer", IntegerSerializer.class.getName());

        ZeroBufferEmitterSender app = runApplication(config, ZeroBufferEmitterSender.class);

        // don't create the topic upfront
        // companion.topics().create(topic, 5);

        toxics().latency("latency", ToxicDirection.UPSTREAM, 5000);

        List<CompletableFuture<Void>> sendAcks = new CopyOnWriteArrayList<>();

        for (int i = 0; i < 10000; i++) {
            int j = i;
            CompletableFuture<Void> future = new CompletableFuture<>();
            sendAcks.add(future);
            // KafkaLogging.log.warn("record :" + j);
            app.send(Message.of(j, () -> {
                // KafkaLogging.log.warn("sent " + j);
                future.complete(null);
                return future;
            }));
        }

        CompletableFuture.allOf(sendAcks.toArray(CompletableFuture[]::new))
                .get(2, TimeUnit.MINUTES);

        toxics().get("latency").remove();

        companion.consumeIntegers().fromTopics(topic, 10000)
                .awaitCompletion();
    }

    @Test
    public void testEmitterNoWaitWriteCompletion()
            throws ExecutionException, InterruptedException, TimeoutException, IOException {

        MapBasedConfig config = kafkaConfig("mp.messaging.outgoing.out")
                .with("topic", topic)
                .with("value.serializer", IntegerSerializer.class.getName())
                .with("waitForWriteCompletion", false);

        EmitterSender app = runApplication(config, EmitterSender.class);

        companion.topics().create(topic, 5);

        toxics().latency("latency", ToxicDirection.UPSTREAM, 5000);

        List<CompletableFuture<Void>> sendAcks = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 10000; i++) {
            int j = i;
            // KafkaLogging.log.warn("record :" + j);
            CompletableFuture<Void> future = new CompletableFuture<>();
            sendAcks.add(future);
            app.send(Message.of(j, () -> {
                // KafkaLogging.log.warn("sent " + j);
                future.complete(null);
                return future;
            }));
        }

        CompletableFuture.allOf(sendAcks.stream().toArray(CompletableFuture[]::new))
                .get(1, TimeUnit.MINUTES);

        toxics().get("latency").remove();

        companion.consumeIntegers().fromTopics(topic, 10000)
                .awaitCompletion();
    }

    @Test
    public void testSynchronizedEmitterVirtualThreaded()
            throws IOException, ExecutionException, InterruptedException, TimeoutException {
        MapBasedConfig config = kafkaConfig("mp.messaging.outgoing.out")
                .with("topic", topic)
                .with("value.serializer", IntegerSerializer.class.getName());

        SynchronizedEmitterSender app = runApplication(config, SynchronizedEmitterSender.class);

        // here not creating the topic upfront causes too much wait
        //        companion.topics().create(topic, 5);

        toxics().latency("latency", ToxicDirection.UPSTREAM, 5000);

        ExecutorService executor = createVirtualThreadExecutorIfAvailable();
        List<CompletableFuture<Void>> sendAcks = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 10000; i++) {
            int j = i;
            CompletableFuture<Void> future = new CompletableFuture<>();
            sendAcks.add(future);
            executor.execute(() -> {
                // KafkaLogging.log.warn("record :" + j);
                app.send(Message.of(j, () -> {
                    // KafkaLogging.log.warn("sent " + j);
                    future.complete(null);
                    return future;
                }));
            });
        }

        CompletableFuture.allOf(sendAcks.toArray(CompletableFuture[]::new))
                .get(2, TimeUnit.MINUTES);

        toxics().get("latency").remove();

        companion.consumeIntegers().fromTopics(topic, 10000)
                .awaitCompletion(Duration.of(2, ChronoUnit.MINUTES));
    }

    @Test
    @Disabled
    public void testEmitterVirtualThreaded()
            throws IOException, ExecutionException, InterruptedException, TimeoutException {
        MapBasedConfig config = kafkaConfig("mp.messaging.outgoing.out")
                .with("topic", topic)
                .with("value.serializer", IntegerSerializer.class.getName());

        EmitterSender app = runApplication(config, EmitterSender.class);

        // here not creating the topic upfront causes too much wait
        //        companion.topics().create(topic, 5);

        toxics().latency("latency", ToxicDirection.UPSTREAM, 5000);

        // This will fail
        ExecutorService executor = createVirtualThreadExecutorIfAvailable();
        List<CompletableFuture<Void>> sendAcks = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 10000; i++) {
            int j = i;
            CompletableFuture<Void> future = new CompletableFuture<>();
            sendAcks.add(future);
            executor.execute(() -> {
                // KafkaLogging.log.warn("record :" + j);
                app.send(Message.of(j, () -> {
                    // KafkaLogging.log.warn("sent " + j);
                    future.complete(null);
                    return future;
                }));
            });
        }

        CompletableFuture.allOf(sendAcks.toArray(CompletableFuture[]::new))
                .get(2, TimeUnit.MINUTES);

        toxics().get("latency").remove();

        companion.consumeIntegers().fromTopics(topic, 10000)
                .awaitCompletion(Duration.of(2, ChronoUnit.MINUTES));
    }

    @Test
    @Disabled
    public void testEmitterMultiThreaded()
            throws IOException, ExecutionException, InterruptedException, TimeoutException {
        MapBasedConfig config = kafkaConfig("mp.messaging.outgoing.out")
                .with("topic", topic)
                .with("value.serializer", IntegerSerializer.class.getName());

        EmitterSender app = runApplication(config, EmitterSender.class);

        // here not creating the topic upfront causes too much wait
        //        companion.topics().create(topic, 5);

        toxics().latency("latency", ToxicDirection.UPSTREAM, 5000);

        // This will probably pass because ThrowingEmitter.requested doesn't take buffer size into account,
        // and will spill into the buffer, without hitting the "Insufficient downstream requests"
        ExecutorService executor = Executors.newFixedThreadPool(100);
        List<CompletableFuture<Void>> sendAcks = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 10000; i++) {
            int j = i;
            CompletableFuture<Void> future = new CompletableFuture<>();
            sendAcks.add(future);
            executor.execute(() -> {
                // KafkaLogging.log.warn("record :" + j);
                app.send(Message.of(j, () -> {
                    // KafkaLogging.log.warn("sent " + j);
                    future.complete(null);
                    return future;
                }));
            });
        }

        CompletableFuture.allOf(sendAcks.toArray(CompletableFuture[]::new))
                .get(2, TimeUnit.MINUTES);

        toxics().get("latency").remove();

        companion.consumeIntegers().fromTopics(topic, 10000)
                .awaitCompletion(Duration.of(2, ChronoUnit.MINUTES));
    }

    @ApplicationScoped
    public static class EmitterSender {

        @Inject
        @Channel("out")
        Emitter<Integer> emitter;

        void send(Message<Integer> value) {
            while (!emitter.hasRequests()) {
                sleep();
            }
            emitter.send(value);
        }

    }

    @ApplicationScoped
    public static class SynchronizedEmitterSender {

        @Inject
        @Channel("out")
        Emitter<Integer> emitter;

        ReentrantLock lock = new ReentrantLock();

        void send(Message<Integer> value) {
            lock.lock();
            try {
                while (!emitter.hasRequests()) {
                    sleep();
                }
                emitter.send(value);
            } catch (Throwable t) {
                throw t;
            } finally {
                lock.unlock();
            }
        }

    }

    @ApplicationScoped
    public static class ZeroBufferEmitterSender {

        @Inject
        @Channel("out")
        @OnOverflow(OnOverflow.Strategy.THROW_EXCEPTION)
        Emitter<Integer> emitter;

        void send(Message<Integer> value) {
            while (!emitter.hasRequests()) {
                sleep();
            }
            emitter.send(value);
        }

    }

    @ApplicationScoped
    public static class BufferedEmitterSender {

        @Inject
        @Channel("out")
        @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 1024)
        Emitter<Integer> emitter;

        void send(Message<Integer> value) {
            while (!emitter.hasRequests()) {
                sleep();
            }
            emitter.send(value);
        }

    }

    public static void sleep() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException ex) {
            KafkaLogging.log.error("Thread was interrupted to send messages to the Kafka topic. " + ex.getMessage());
            Thread.currentThread().interrupt();
        }
    }


    ExecutorService createVirtualThreadExecutorIfAvailable() {
        try {
            Method newVirtualThreadPerTaskExecutor = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
            return (ExecutorService) newVirtualThreadPerTaskExecutor.invoke(null);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            KafkaLogging.log.error("Cannot create virtual thread executor, falling back to platform thread pool");
            return Executors.newFixedThreadPool(100);
        }
    }
}
