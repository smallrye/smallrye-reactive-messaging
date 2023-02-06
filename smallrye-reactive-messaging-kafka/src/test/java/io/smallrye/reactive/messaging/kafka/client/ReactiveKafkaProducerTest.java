package io.smallrye.reactive.messaging.kafka.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ReactiveKafkaProducerTest extends ClientTestBase {

    @Test
    void testConfigClientIdPrefix() {
        MapBasedConfig producerConfig = createProducerConfig()
                .put("channel-name", "test");
        KafkaSink sink = createSink(producerConfig);
        String clientId = (String) sink.getProducer().configuration().get(ProducerConfig.CLIENT_ID_CONFIG);
        assertThat(clientId).isEqualTo("kafka-producer-test");

        producerConfig.put("client.id", "my-producer");
        sink = createSink(producerConfig);
        clientId = (String) sink.getProducer().configuration().get(ProducerConfig.CLIENT_ID_CONFIG);
        assertThat(clientId).isEqualTo("my-producer");

        producerConfig.put("client-id-prefix", "my-custom-");
        producerConfig.remove("client.id");
        sink = createSink(producerConfig);
        clientId = (String) sink.getProducer().configuration().get(ProducerConfig.CLIENT_ID_CONFIG);
        assertThat(clientId).isEqualTo("my-custom-test");

        producerConfig.put("client.id", "my-producer");
        producerConfig.put("client-id-prefix", "my-custom-");
        sink = createSink(producerConfig);
        clientId = (String) sink.getProducer().configuration().get(ProducerConfig.CLIENT_ID_CONFIG);
        assertThat(clientId).isEqualTo("my-custom-my-producer");
    }

    @Test
    public void independentProducerSingleThreadSingleKey() throws InterruptedException {
        // can pass either `true` or `false` as the last argument, doesn't make a difference
        independentProducerTest(1, 400, false);
    }

    @Test
    public void independentProducerMultipleThreadsSingleKey() throws InterruptedException {
        independentProducerTest(4, 100, false);
    }

    @Test
    public void independentProducerMultipleThreadsMultipleKeys() throws InterruptedException {
        independentProducerTest(4, 100, true);
    }

    private void independentProducerTest(int numberOfThreads, int numberOfMessagesPerThread,
            boolean uniqueKeyPerThread) throws InterruptedException {

        ConsumerTask<Integer, String> records = companion.consume(Integer.class, String.class)
                .fromTopics(topic, (long) numberOfThreads * numberOfMessagesPerThread, Duration.ofMinutes(1));

        Queue<Thread> threads = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < numberOfThreads; i++) {
            IndependentProducerThread thread = new IndependentProducerThread("T" + i, uniqueKeyPerThread ? i : 1,
                    numberOfMessagesPerThread);
            threads.add(thread);
            thread.start();
        }

        records.awaitCompletion(Duration.ofMinutes(1));

        for (int i = 0; i < numberOfThreads; i++) {
            assertThat(records.getRecords())
                    .extracting(ConsumerRecord::value)
                    .containsAll(expectedMessages("T" + i, numberOfMessagesPerThread));
        }

        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    public void sharedProducerSingleThreadSingleKey() throws InterruptedException {
        // can pass either `true` or `false` as the last argument, doesn't make a difference
        sharedProducerTest(1, 400, false);
    }

    @Test
    public void sharedProducerMultipleThreadsSingleKey() throws InterruptedException {
        sharedProducerTest(4, 100, false);
    }

    @Test
    public void sharedProducerMultipleThreadsMultipleKeys() throws InterruptedException {
        sharedProducerTest(4, 100, true);
    }

    private void sharedProducerTest(int numberOfThreads, int numberOfMessagesPerThread,
            boolean uniqueKeyPerThread) throws InterruptedException {
        ConsumerTask<Integer, String> records = companion.consume(Integer.class, String.class)
                .fromTopics(topic, (long) numberOfThreads * numberOfMessagesPerThread, Duration.ofMinutes(1));

        Queue<Thread> threads = new ConcurrentLinkedQueue<>();
        Queue<Multi<Message<?>>> multis = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < numberOfThreads; i++) {
            int finalI = i;
            Multi<Message<?>> multi = Multi.createFrom().emitter(em -> {
                SharedProducerThread thread = new SharedProducerThread("T" + finalI, uniqueKeyPerThread ? finalI : 1,
                        numberOfMessagesPerThread, (MultiEmitter<Message<?>>) em);
                threads.add(thread);
                thread.start();
            });
            multis.add(multi);
        }

        Thread actualProducer = new Thread(() -> {
            Multi<Message<?>> merge = Multi.createBy().merging().streams(multis);
            Flow.Subscriber<Message<?>> subscriber = (Flow.Subscriber<Message<?>>) createSink().getSink();
            merge.subscribe().withSubscriber(subscriber);
        });
        threads.add(actualProducer);
        actualProducer.start();

        records.awaitCompletion(Duration.ofMinutes(1));

        for (int i = 0; i < numberOfThreads; i++) {
            assertThat(records.getRecords())
                    .extracting(ConsumerRecord::value)
                    .containsAll(expectedMessages("T" + i, numberOfMessagesPerThread));
        }

        for (Thread thread : threads) {
            thread.join();
        }
    }

    private List<String> expectedMessages(String threadId, int count) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(threadId + ":M" + i);
        }
        return result;
    }

    public KafkaSink createSink() {
        MapBasedConfig config = createProducerConfig()
                .put("channel-name", "test-" + ThreadLocalRandom.current().nextInt())
                .put("topic", topic);

        KafkaSink sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), UnsatisfiedInstance.instance());
        this.sinks.add(sink);
        return sink;
    }

    public KafkaSink createSink(MapBasedConfig config) {
        KafkaSink sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), UnsatisfiedInstance.instance());
        this.sinks.add(sink);
        return sink;
    }

    public KafkaSink createTransactionalSink() {
        String channelName = "test-" + ThreadLocalRandom.current().nextInt();
        MapBasedConfig config = createProducerConfig()
                .with("channel-name", channelName)
                .with(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-producer")
                .with(ProducerConfig.ACKS_CONFIG, "all")
                .with("topic", topic);

        KafkaSink sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), UnsatisfiedInstance.instance());
        this.sinks.add(sink);
        return sink;
    }

    private class IndependentProducerThread extends Thread {
        private final String threadId;
        private final int messageKey; // used for all messages produced by this thread, to guarantee ordering
        private final int messageCount;

        IndependentProducerThread(String threadId, int messageKey, int messageCount) {
            this.threadId = threadId;
            this.messageKey = messageKey;
            this.messageCount = messageCount;
            this.setName(ReactiveKafkaProducerTest.class.getSimpleName() + "-" + threadId);
        }

        @Override
        public void run() {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(20));
            } catch (InterruptedException e) {
                return;
            }

            Multi<Message<?>> stream = Multi.createFrom().emitter(emitter -> {
                for (int i = 0; i < messageCount; i++) {
                    emitter.emit(KafkaRecord.of(messageKey, threadId + ":M" + i));

                    try {
                        Thread.sleep(ThreadLocalRandom.current().nextInt(20));
                    } catch (InterruptedException e) {
                        break;
                    }
                }

                emitter.complete();
            });

            Flow.Subscriber<Message<?>> subscriber = (Flow.Subscriber<Message<?>>) createSink().getSink();
            stream.subscribe().withSubscriber(subscriber);
        }
    }

    private static class SharedProducerThread extends Thread {
        private final String threadId;
        private final int messageKey; // used for all messages produced by this thread, to guarantee ordering
        private final int messageCount;
        private final MultiEmitter<Message<?>> emitter;

        SharedProducerThread(String threadId, int messageKey, int messageCount, MultiEmitter<Message<?>> emitter) {
            this.threadId = threadId;
            this.messageKey = messageKey;
            this.messageCount = messageCount;
            this.emitter = emitter;
            this.setName(ReactiveKafkaProducerTest.class.getSimpleName() + "-" + threadId);
        }

        @Override
        public void run() {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(20));
            } catch (InterruptedException e) {
                return;
            }

            for (int i = 0; i < messageCount; i++) {
                emitter.emit(KafkaRecord.of(messageKey, threadId + ":M" + i));

                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(20));
                } catch (InterruptedException e) {
                    break;
                }
            }

            emitter.complete();
        }
    }

}
