package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.base.MapBasedConfig;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

/**
 * Kafka Source Concurrency Experiment:
 * <p>
 * Hypothesis: If we match a topic's partition count with consumer instances
 * then we should see concurrent record processing.
 * <p>
 * We will use topics with 2 partitions.
 * We will send 2 records with keys "1" and "2".
 * We will test that both records are processed and done so through separate partitions within 25 seconds.
 * <p>
 * For control we will provide a bean that will process both records as quickly as possible.
 * For the experiment we will provide beans that purposefully sleep for 30 seconds after processing (prevent ack)
 * <p>
 * The idea here is that a force sleep should only delay the processing of records from 1 partition and not all.
 */
class KafkaSourceConcurrencyTest extends KafkaTestBase {

    private static final int RECORD_COUNT = 2;

    private interface ExperimentBean {
        List<IncomingKafkaRecord<String, Integer>> getResults();

        int getEventThreadCount();
    }

    private static void waitFor30SecondsOrAllRecordsProcessed(ExperimentBean bean) {
        await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> bean.getResults().size() >= RECORD_COUNT);
    }

    /**
     * Control bean
     * <p>
     * processes the record and ack as quickly as possible
     */
    @ApplicationScoped
    public static class ControlBean implements ExperimentBean {

        private final List<IncomingKafkaRecord<String, Integer>> list = new CopyOnWriteArrayList<>();

        private final Set<Long> threadsUsed = ConcurrentHashMap.newKeySet();

        @Override
        public List<IncomingKafkaRecord<String, Integer>> getResults() {
            return list;
        }

        @Override
        public int getEventThreadCount() {
            return threadsUsed.size();
        }

        @Incoming("data")
        public CompletionStage<Void> process(IncomingKafkaRecord<String, Integer> input) {
            list.add(input);
            threadsUsed.add(Thread.currentThread().getId());
            return input.ack();
        }
    }

    /**
     * Experiment bean that uses {@link Acknowledgment.Strategy.POST_PROCESSING}
     * <p>
     * This tests an incoming channel with defaults
     */
    @ApplicationScoped
    public static class PostAckWith30SecondSleepBean implements ExperimentBean {

        private final List<IncomingKafkaRecord<String, Integer>> list = new CopyOnWriteArrayList<>();

        private final Set<Long> threadsUsed = ConcurrentHashMap.newKeySet();

        @Override
        public List<IncomingKafkaRecord<String, Integer>> getResults() {
            return list;
        }

        @Override
        public int getEventThreadCount() {
            return threadsUsed.size();
        }

        @Incoming("data")
        public CompletionStage<Void> process(IncomingKafkaRecord<String, Integer> input) {
            list.add(input);
            threadsUsed.add(Thread.currentThread().getId());
            waitFor30SecondsOrAllRecordsProcessed(this);
            return input.ack();
        }
    }

    /**
     * Experiment bean that uses {@link Acknowledgment.Strategy.POST_PROCESSING}
     * <p>
     * This tests an incoming channel with defaults
     * This tests processing in a worker thread
     */
    @ApplicationScoped
    public static class PostAckWithWorkerThreadWith30SecondSleepBean implements ExperimentBean {

        private final List<IncomingKafkaRecord<String, Integer>> list = new CopyOnWriteArrayList<>();

        private final Executor executor = Executors.newFixedThreadPool(8);

        private final Set<Long> threadsUsed = ConcurrentHashMap.newKeySet();

        @Override
        public List<IncomingKafkaRecord<String, Integer>> getResults() {
            return list;
        }

        @Override
        public int getEventThreadCount() {
            return threadsUsed.size();
        }

        @Incoming("data")
        public CompletionStage<Void> process(IncomingKafkaRecord<String, Integer> input) {
            threadsUsed.add(Thread.currentThread().getId());
            return CompletableFuture
                    .runAsync(() -> {
                        list.add(input);
                        waitFor30SecondsOrAllRecordsProcessed(this);
                    },
                            executor);
        }
    }

    /**
     * Experiment bean that uses {@link Acknowledgment.Strategy.NONE}
     * <p>
     * The incoming method here returns a CompletableFuture.completedFuture(null) (fire and forget)
     * It does all processing inside a worker thread (1 thread per partition).
     */
    @ApplicationScoped
    public static class NoneAckWith30SecondSleepBean implements ExperimentBean {

        private final List<IncomingKafkaRecord<String, Integer>> list = new CopyOnWriteArrayList<>();

        private final Map<Integer, Executor> partitionExecutors = new ConcurrentHashMap<>();

        private final Set<Long> threadsUsed = ConcurrentHashMap.newKeySet();

        @Override
        public List<IncomingKafkaRecord<String, Integer>> getResults() {
            return list;
        }

        @Override
        public int getEventThreadCount() {
            return threadsUsed.size();
        }

        private Executor getExecutor(int partition) {
            Executor executor = partitionExecutors.get(partition);
            if (executor == null) {
                executor = Executors.newSingleThreadExecutor((r) -> new Thread(r, "worker-partition-" + partition));
                partitionExecutors.put(partition, executor);
            }
            return executor;
        }

        @Incoming("data")
        @Acknowledgment(Acknowledgment.Strategy.NONE)
        public CompletionStage<Void> process(IncomingKafkaRecord<String, Integer> input) {
            threadsUsed.add(Thread.currentThread().getId());
            CompletableFuture
                    .runAsync(() -> {
                        list.add(input);
                        waitFor30SecondsOrAllRecordsProcessed(this);
                        input.ack();
                    },
                            getExecutor(input.getPartition()));
            return CompletableFuture.completedFuture(null);
        }
    }

    KafkaSource<String, Integer> source;
    KafkaConnector connector;

    @AfterEach
    public void closing() {
        if (source != null) {
            source.closeQuietly();
        }
        if (connector != null) {
            connector.terminate(new Object());
        }
    }

    private MapBasedConfig myKafkaSourceConfig(int partitions, String topic) {
        MapBasedConfig.Builder builder = MapBasedConfig.builder("mp.messaging.incoming.data");
        builder.put("value.deserializer", IntegerDeserializer.class.getName());
        builder.put("enable.auto.commit", "false");
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", topic);
        builder.put("consumer-rebalance-listener.name", "ResetToZeroRebalanceListener");
        if (partitions > 1) {
            builder.put("partitions", Integer.toString(partitions));
        }

        return builder.build();
    }

    /**
     * Experiment runner.
     * <p>
     * Will send two records
     * Will test that both records get processed within 25 seconds
     * Will test that both records arrived in separate partitions.
     *
     * @param consumerBeanType - experiments parameters
     * @param clientConsumerCount - number of consumer clients to configure
     * @param <T>
     */
    private <T extends ExperimentBean> void test(Class<T> consumerBeanType, int clientConsumerCount) {
        String topicName = "data-concurrency-experiment-" + UUID.randomUUID().toString();
        createTopic(topicName, 2);
        T bean = run(
                myKafkaSourceConfig(clientConsumerCount, topicName),
                consumerBeanType);

        ResetToZeroRebalanceListener listener = get(ResetToZeroRebalanceListener.class);

        // we need two wait for all consumers to connect before producing
        await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertEquals(clientConsumerCount, listener.getTopicPartitionsByThread().size());
                    // if we have two client consumers then each event thread should be assigned one partition
                    if (clientConsumerCount == 2) {
                        listener.getTopicPartitionsByThread()
                                .values()
                                .forEach(topicPartitions -> assertEquals(1, topicPartitions.size()));
                    }
                });

        List<IncomingKafkaRecord<String, Integer>> list = bean.getResults();
        assertThat(list).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(RECORD_COUNT, null,
                () -> new ProducerRecord<>(topicName, String.valueOf(counter.get()), counter.getAndIncrement()))).start();

        await()
                .atMost(25, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(RECORD_COUNT, list.size(), "records consumed"));

        Set<Integer> usedPartitions = new HashSet<>();
        list
                .forEach(record -> {
                    assertTrue(usedPartitions.add(record.getPartition()));
                });

        assertEquals(clientConsumerCount, bean.getEventThreadCount(), "consumed in event threads");
    }

    /**
     * control test
     * <p>
     * consumer count: 2
     * <p>
     * results: PASS
     */
    @Test
    void testControlBean() {
        test(ControlBean.class, 2);
    }

    /**
     * test {@link NoneAckWith30SecondSleepBean} with 2 kafka consumers
     * <p>
     * consumer count: 2
     * <p>
     * results: PASS
     */
    @Test
    void testNoneAckWith30SecondSleepBean() {
        test(NoneAckWith30SecondSleepBean.class, 2);
    }

    /**
     * test {@link NoneAckWith30SecondSleepBean} with 1 kafka consumers
     * <p>
     * consumer count: 1
     * <p>
     * results: PASS
     * <p>
     * This result is interesting. It shows that we can design a processor that is decoupled from the consumer.
     * We can have 1 consumer/client but still have a thread per partition that processes records in order
     * and sequentially per partition.
     */
    @Test
    void testNoneAckWith30SecondSleepWithSingleClientBean() {
        test(NoneAckWith30SecondSleepBean.class, 1);
    }

    /**
     * Test incoming channel with defaults
     * <p>
     * consumer count: 2
     * <p>
     * results: FAIL
     * <p>
     * With the default ack {@link Acknowledgment.Strategy.POST_PROCESSING} it looks
     * like records are only delivered in series irrespective of consumer count.
     */
    @Test
    void testPostAck30SecondSleep() {
        test(PostAckWith30SecondSleepBean.class, 2);
    }

    /**
     * Test incoming channel with defaults and processing in a worker thread
     * <p>
     * consumer count: 2
     * <p>
     * results: FAIL
     * <p>
     * With the default ack {@link Acknowledgment.Strategy.POST_PROCESSING} it looks
     * like records are only delivered in series irrespective of consumer count.
     */
    @Test
    void testPostAckWorkerThread30SecondSleep() {
        test(PostAckWithWorkerThreadWith30SecondSleepBean.class, 2);
    }

    private <T> T run(MapBasedConfig config, Class<T> beanType) {
        addBeans(beanType, ResetToZeroRebalanceListener.class);
        runApplication(config);
        return get(beanType);
    }

    @ApplicationScoped
    @Named("ResetToZeroRebalanceListener")
    public static class ResetToZeroRebalanceListener implements KafkaConsumerRebalanceListener {

        private final Map<Long, Set<TopicPartition>> topicPartitionsByThread = new ConcurrentHashMap<>();

        @Override
        public Uni<Void> onPartitionsAssigned(KafkaConsumer<?, ?> consumer, Set<TopicPartition> topicPartitions) {
            topicPartitionsByThread.put(Thread.currentThread().getId(), topicPartitions);
            return consumer.seekToBeginning(topicPartitions);
        }

        @Override
        public Uni<Void> onPartitionsRevoked(KafkaConsumer<?, ?> consumer, Set<TopicPartition> topicPartitions) {
            topicPartitionsByThread.remove(Thread.currentThread().getId());
            return Uni
                    .createFrom()
                    .nullItem();
        }

        public Map<Long, Set<TopicPartition>> getTopicPartitionsByThread() {
            return topicPartitionsByThread;
        }
    }
}
