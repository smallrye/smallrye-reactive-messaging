package io.smallrye.reactive.messaging.kafka.transactions;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.KafkaProducer;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaRecordBatch;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordBatchMetadata;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.converters.ConsumerRecordConverter;
import io.smallrye.reactive.messaging.kafka.converters.ConsumerRecordsConverter;
import io.smallrye.reactive.messaging.kafka.impl.PooledKafkaProducer;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PerPartitionExactlyOnceProcessingTest extends KafkaCompanionTestBase {

    String inTopic;
    String outTopic;

    @Test
    void testPerPartitionExactlyOnceProcessor() {
        inTopic = companion.topics().createAndWait(topic, 3);
        outTopic = companion.topics().createAndWait(topic + "-out", 3);
        int numberOfRecords = 30;
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(consumerConfig());
        PerPartitionExactlyOnceProcessor application = runApplication(config, PerPartitionExactlyOnceProcessor.class);

        // Send records distributed across 3 partitions
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(inTopic, i % 3, "k" + i, i), numberOfRecords);

        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .withOffsetReset(OffsetResetStrategy.EARLIEST)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(outTopic, numberOfRecords)
                .awaitCompletion(Duration.ofMinutes(1));

        assertThat(records.getRecords())
                .extracting(ConsumerRecord::value)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();

        assertThat(application.getProcessed())
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();
    }

    @Test
    void testPerPartitionProducersCreatedPerPartition() {
        inTopic = companion.topics().createAndWait(topic, 3);
        outTopic = companion.topics().createAndWait(topic + "-out", 3);
        int numberOfRecords = 30;
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(consumerConfig());
        runApplication(config, PerPartitionExactlyOnceProcessor.class);

        // Send records distributed across 3 partitions
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(inTopic, i % 3, "k" + i, i), numberOfRecords);

        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .withOffsetReset(OffsetResetStrategy.EARLIEST)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(outTopic, numberOfRecords)
                .awaitCompletion(Duration.ofMinutes(1));

        assertThat(records.getRecords())
                .extracting(ConsumerRecord::value)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();

        // Verify pooled producer structure
        KafkaClientService clientService = get(KafkaClientService.class);
        KafkaProducer<?, ?> producer = clientService.getProducer("transactional-producer");
        assertThat(producer).isInstanceOf(PooledKafkaProducer.class);

        PooledKafkaProducer<?, ?> pooledProducer = (PooledKafkaProducer<?, ?>) producer;
        // Verify that producers were created in the pool
        assertThat(pooledProducer.getProducers()).isNotEmpty();

        // Verify each inner producer has a distinct transactional.id
        Set<String> transactionalIds = pooledProducer.getProducers().stream()
                .map(p -> (String) p.configuration().get(ProducerConfig.TRANSACTIONAL_ID_CONFIG))
                .collect(Collectors.toSet());
        assertThat(transactionalIds).hasSameSizeAs(pooledProducer.getProducers());
        // All transactional IDs should be based on the base "tx-producer" with index suffix
        transactionalIds.forEach(txId -> assertThat(txId).startsWith("tx-producer-"));
    }

    @Test
    void testAbortOnOnePartitionDoesNotAffectOthers() {
        inTopic = companion.topics().createAndWait(topic, 3);
        outTopic = companion.topics().createAndWait(topic + "-out", 3);
        int numberOfRecords = 30;
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(consumerConfig());
        AbortOnPartitionProcessor application = runApplication(config, AbortOnPartitionProcessor.class);

        // Send records distributed across 3 partitions
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(inTopic, i % 3, "k" + i, i), numberOfRecords);

        // Records from partition 1 on first attempt are aborted, but retried.
        // All records should eventually be committed.
        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .withOffsetReset(OffsetResetStrategy.EARLIEST)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(outTopic, numberOfRecords)
                .awaitCompletion(Duration.ofMinutes(1));

        assertThat(records.getRecords())
                .extracting(ConsumerRecord::value)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();

        // Verify abort happened on partition 1
        assertThat(application.getAbortOccurred()).isTrue();
        assertThat(application.transaction().isTransactionInProgress()).isFalse();
    }

    @Test
    void testConcurrentPerPartitionTransactions() {
        int numberOfRecords = 4000;
        int partitions = 5;
        inTopic = companion.topics().createAndWait(topic, partitions);
        outTopic = companion.topics().createAndWait(topic + "-out", partitions);
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(consumerConfig()
                .with("concurrency", partitions));
        ConcurrentPerPartitionProcessor application = runApplication(config,
                ConcurrentPerPartitionProcessor.class);

        // Send records distributed across 3 partitions
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(inTopic, i % partitions, "k" + i, i), numberOfRecords);

        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .withOffsetReset(OffsetResetStrategy.EARLIEST)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(outTopic, numberOfRecords)
                .awaitCompletion(Duration.ofSeconds(20));

        assertThat(records.getRecords())
                .extracting(ConsumerRecord::value)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();

        // Verify that transactions actually ran concurrently
        assertThat(application.getMaxConcurrency())
                .as("Max concurrent transactions should be > 1, proving pooled scopes run in parallel")
                .isGreaterThan(1);
        assertThat(application.transaction().isTransactionInProgress()).isFalse();
    }

    @Test
    void testConcurrentAbortDoesNotCauseDuplicates() {
        int numberOfRecords = 100;
        int partitions = 3;
        inTopic = companion.topics().createAndWait(topic, partitions);
        outTopic = companion.topics().createAndWait(topic + "-out", partitions);
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(consumerConfig()
                .with("concurrency", partitions));
        ConcurrentAbortProcessor application = runApplication(config, ConcurrentAbortProcessor.class);

        // Send records distributed across partitions
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(inTopic, i % partitions, "k" + i, i), numberOfRecords);

        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .withOffsetReset(OffsetResetStrategy.EARLIEST)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(outTopic, numberOfRecords)
                .awaitCompletion(Duration.ofMinutes(1));

        // Key assertion: no duplicates despite concurrent abort + retry
        assertThat(records.getRecords())
                .extracting(ConsumerRecord::value)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();

        // Verify abort actually happened and processing was concurrent
        assertThat(application.getAbortCount()).isGreaterThan(0);
        assertThat(application.getMaxConcurrency())
                .as("Processing should be concurrent")
                .isGreaterThan(1);
        assertThat(application.transaction().isTransactionInProgress()).isFalse();
    }

    @Test
    void testPerPartitionWithIncomingMetadata() {
        inTopic = companion.topics().createAndWait(topic, 3);
        outTopic = companion.topics().createAndWait(topic + "-out", 3);
        int numberOfRecords = 30;
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(consumerConfig());
        MetadataOverloadProcessor application = runApplication(config, MetadataOverloadProcessor.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(inTopic, i % 3, "k" + i, i), numberOfRecords);

        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .withOffsetReset(OffsetResetStrategy.EARLIEST)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(outTopic, numberOfRecords)
                .awaitCompletion(Duration.ofMinutes(1));

        assertThat(records.getRecords())
                .extracting(ConsumerRecord::value)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();

        assertThat(application.getProcessed())
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();
    }

    @Test
    void testPerPartitionWithBatchMetadata() {
        inTopic = companion.topics().createAndWait(topic, 3);
        outTopic = companion.topics().createAndWait(topic + "-out", 3);
        int numberOfRecords = 30;
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(batchConsumerConfig());
        BatchMetadataOverloadProcessor application = runApplication(config, BatchMetadataOverloadProcessor.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(inTopic, i % 3, "k" + i, i), numberOfRecords);

        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .withOffsetReset(OffsetResetStrategy.EARLIEST)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(outTopic, numberOfRecords)
                .awaitCompletion(Duration.ofMinutes(1));

        assertThat(records.getRecords())
                .extracting(ConsumerRecord::value)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();

        assertThat(application.getProcessed())
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();
    }

    @Test
    void testBlockingOrderedPartitionWithExactlyOnce() {
        addBeans(ConsumerRecordConverter.class);
        int partitions = 3;
        inTopic = companion.topics().createAndWait(topic, partitions);
        outTopic = companion.topics().createAndWait(topic + "-out", partitions);
        int numberOfRecords = 100;
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(throttledPartitionOrderedConsumerConfig());
        config.put("smallrye.messaging.worker.tx-pool.max-concurrency", partitions);
        BlockingPartitionOrderedProcessor application = runApplication(config, BlockingPartitionOrderedProcessor.class);

        // Send records distributed across partitions
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(inTopic, i % partitions, "k" + i, i), numberOfRecords);

        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .withOffsetReset(OffsetResetStrategy.EARLIEST)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(outTopic, numberOfRecords)
                .awaitCompletion(Duration.ofMinutes(2));

        assertThat(records.getRecords())
                .extracting(ConsumerRecord::value)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();

        assertThat(application.getProcessed())
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();

        // Verify concurrent processing happened across partitions
        assertThat(application.getMaxConcurrency())
                .as("Max concurrent transactions should be > 1, proving cross-partition parallelism")
                .isGreaterThan(1);
        assertThat(application.transaction().isTransactionInProgress()).isFalse();

        // Verify per-partition max concurrency was 1
        // (throttled.ordered=partition guarantees sequential processing within a partition)
        application.getPartitionMaxConcurrency().forEach((partition, maxConc) -> {
            assertThat(maxConc.get())
                    .as("Max concurrency for partition %d should be 1", partition)
                    .isEqualTo(1);
        });
    }

    @Test
    void testWithTransactionWithoutMessage() {
        outTopic = companion.topics().createAndWait(topic, 3);
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        WithTransactionNoMessageApp app = runApplication(config, WithTransactionNoMessageApp.class);

        // withTransaction(Function) without an incoming message should work with pooled producer
        app.produceWithoutMessage().await().indefinitely();

        companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(outTopic, 1)
                .awaitCompletion(Duration.ofMinutes(1));
    }

    @Test
    void testWithTransactionAndAwaitMetadata() {
        addBeans(ConsumerRecordConverter.class);
        inTopic = companion.topics().createAndWait(topic, 3);
        outTopic = companion.topics().createAndWait(topic + "-out", 3);
        int numberOfRecords = 30;
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(orderedPartitionConsumerConfig());
        config.put("smallrye.messaging.worker.tx-pool.max-concurrency", 3);
        MetadataAndAwaitProcessor application = runApplication(config, MetadataAndAwaitProcessor.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(inTopic, i % 3, "k" + i, i), numberOfRecords);

        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .withOffsetReset(OffsetResetStrategy.EARLIEST)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(outTopic, numberOfRecords)
                .awaitCompletion();

        assertThat(records.getRecords())
                .extracting(ConsumerRecord::value)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();

        assertThat(application.getProcessed())
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();
    }

    @Test
    void testWithTransactionAndAwaitBatchMetadata() {
        addBeans(ConsumerRecordsConverter.class);
        inTopic = companion.topics().createAndWait(topic, 3);
        outTopic = companion.topics().createAndWait(topic + "-out", 3);
        int numberOfRecords = 30;
        MapBasedConfig config = new MapBasedConfig(producerConfig());
        config.putAll(batchConsumerConfig());
        BatchMetadataAndAwaitProcessor application = runApplication(config, BatchMetadataAndAwaitProcessor.class);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(inTopic, i % 3, "k" + i, i), numberOfRecords);

        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .withOffsetReset(OffsetResetStrategy.EARLIEST)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(outTopic, numberOfRecords)
                .awaitCompletion();

        assertThat(records.getRecords())
                .extracting(ConsumerRecord::value)
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();

        assertThat(application.getProcessed())
                .containsAll(IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList()))
                .doesNotHaveDuplicates();
    }

    private KafkaMapBasedConfig producerConfig() {
        return kafkaConfig("mp.messaging.outgoing.transactional-producer")
                .with("topic", outTopic)
                .with("transactional.id", "tx-producer")
                .with("acks", "all")
                .with("pooled-producer", true)
                .with("value.serializer", IntegerSerializer.class.getName());
    }

    private KafkaMapBasedConfig batchConsumerConfig() {
        return kafkaConfig("mp.messaging.incoming.exactly-once-consumer")
                .with("topic", inTopic)
                .with("group.id", "my-consumer")
                .with("commit-strategy", "ignore")
                .with("failure-strategy", "ignore")
                .with("batch", true)
                .with("max.poll.records", "100")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", IntegerDeserializer.class.getName());
    }

    private KafkaMapBasedConfig consumerConfig() {
        return kafkaConfig("mp.messaging.incoming.exactly-once-consumer")
                .with("topic", inTopic)
                .with("group.id", "my-consumer")
                .with("commit-strategy", "ignore")
                .with("failure-strategy", "ignore")
                .with("max.poll.records", "100")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", IntegerDeserializer.class.getName());
    }

    private KafkaMapBasedConfig throttledPartitionOrderedConsumerConfig() {
        return kafkaConfig("mp.messaging.incoming.exactly-once-consumer")
                .with("topic", inTopic)
                .with("group.id", "my-consumer")
                .with("commit-strategy", "throttled")
                .with("throttled.ordered", "partition")
                .with("failure-strategy", "ignore")
                .with("max.poll.records", "100")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", IntegerDeserializer.class.getName());
    }

    @ApplicationScoped
    public static class WithTransactionNoMessageApp {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        Uni<Void> produceWithoutMessage() {
            return transaction.withTransaction(emitter -> {
                emitter.send(1);
                return Uni.createFrom().voidItem();
            });
        }
    }

    @ApplicationScoped
    public static class PerPartitionExactlyOnceProcessor {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        List<Integer> processed = new CopyOnWriteArrayList<>();

        @Incoming("exactly-once-consumer")
        Uni<Void> process(KafkaRecord<String, Integer> record) {
            return transaction.withTransaction(record, emitter -> {
                // No explicit partition needed — falls back to incoming record's partition
                emitter.send(KafkaRecord.of(record.getKey(), record.getPayload()));
                processed.add(record.getPayload());
                return Uni.createFrom().voidItem();
            });
        }

        public List<Integer> getProcessed() {
            return processed;
        }
    }

    @ApplicationScoped
    public static class AbortOnPartitionProcessor {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        final AtomicBoolean abortOccurred = new AtomicBoolean(false);
        // Track which values from partition 1 already caused an error to avoid infinite retry
        final ConcurrentHashMap<Integer, Boolean> erroredOnce = new ConcurrentHashMap<>();

        List<Integer> processed = new CopyOnWriteArrayList<>();

        @Incoming("exactly-once-consumer")
        Uni<Void> process(KafkaRecord<String, Integer> record) {
            return transaction.withTransaction(record, emitter -> {
                // On partition 1, fail the first time for the first record seen
                if (record.getPartition() == 1 && erroredOnce.putIfAbsent(record.getPayload(), true) == null) {
                    abortOccurred.set(true);
                    throw new RuntimeException("Simulated failure on partition 1 for value " + record.getPayload());
                }
                emitter.send(KafkaRecord.of(record.getKey(), record.getPayload()));
                processed.add(record.getPayload());
                return Uni.createFrom().voidItem();
            }).onFailure().recoverWithUni(t -> Uni.createFrom().completionStage(record.nack(t)));
        }

        public boolean getAbortOccurred() {
            return abortOccurred.get();
        }

        public List<Integer> getProcessed() {
            return processed;
        }

        public KafkaTransactions<Integer> transaction() {
            return transaction;
        }
    }

    @ApplicationScoped
    public static class ConcurrentAbortProcessor {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        final ConcurrentHashMap<Integer, Boolean> erroredOnce = new ConcurrentHashMap<>();
        final AtomicInteger abortCount = new AtomicInteger(0);
        final AtomicInteger concurrency = new AtomicInteger(0);
        final AtomicInteger maxConcurrency = new AtomicInteger(0);

        @Incoming("exactly-once-consumer")
        Uni<Void> process(KafkaRecord<String, Integer> record) {
            int current = concurrency.incrementAndGet();
            maxConcurrency.updateAndGet(max -> Math.max(max, current));
            return transaction.withTransaction(record, emitter -> {
                // On partition 1, fail the first time for each record to trigger abort
                if (record.getPartition() == 1 && erroredOnce.putIfAbsent(record.getPayload(), true) == null) {
                    abortCount.incrementAndGet();
                    throw new RuntimeException(
                            "Simulated failure on partition 1 for value " + record.getPayload());
                }
                emitter.send(KafkaRecord.of(record.getKey(), record.getPayload()));
                return Uni.createFrom().voidItem();
            }).onFailure().recoverWithUni(t -> Uni.createFrom().completionStage(record.nack(t)))
                    .eventually(() -> concurrency.decrementAndGet());
        }

        public int getAbortCount() {
            return abortCount.get();
        }

        public int getMaxConcurrency() {
            return maxConcurrency.get();
        }

        public KafkaTransactions<Integer> transaction() {
            return transaction;
        }
    }

    @ApplicationScoped
    public static class ConcurrentPerPartitionProcessor {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        List<Integer> processed = new CopyOnWriteArrayList<>();
        AtomicInteger concurrency = new AtomicInteger(0);
        AtomicInteger maxConcurrency = new AtomicInteger(0);

        @Incoming("exactly-once-consumer")
        Uni<Void> process(KafkaRecord<String, Integer> record) {
            int current = concurrency.incrementAndGet();
            maxConcurrency.updateAndGet(max -> Math.max(max, current));
            return transaction.withTransaction(record, emitter -> {
                emitter.send(KafkaRecord.of(record.getKey(), record.getPayload()));
                processed.add(record.getPayload());
                return Uni.createFrom().voidItem();
            }).eventually(() -> concurrency.decrementAndGet());
        }

        public List<Integer> getProcessed() {
            return processed;
        }

        public int getMaxConcurrency() {
            return maxConcurrency.get();
        }

        public KafkaTransactions<Integer> transaction() {
            return transaction;
        }
    }

    @ApplicationScoped
    public static class MetadataOverloadProcessor {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        List<Integer> processed = new CopyOnWriteArrayList<>();

        @Incoming("exactly-once-consumer")
        @SuppressWarnings("unchecked")
        Uni<Void> process(KafkaRecord<String, Integer> record) {
            IncomingKafkaRecordMetadata<String, Integer> metadata = record
                    .getMetadata(IncomingKafkaRecordMetadata.class)
                    .orElseThrow();
            return transaction.withTransaction(metadata, emitter -> {
                emitter.send(KafkaRecord.of(record.getKey(), record.getPayload()));
                processed.add(record.getPayload());
                return Uni.createFrom().voidItem();
            });
        }

        public List<Integer> getProcessed() {
            return processed;
        }
    }

    @ApplicationScoped
    public static class BatchMetadataOverloadProcessor {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        List<Integer> processed = new CopyOnWriteArrayList<>();

        @Incoming("exactly-once-consumer")
        @SuppressWarnings("unchecked")
        Uni<Void> process(KafkaRecordBatch<String, Integer> batch) {
            IncomingKafkaRecordBatchMetadata<String, Integer> metadata = batch
                    .getMetadata(IncomingKafkaRecordBatchMetadata.class)
                    .orElseThrow();
            return transaction.withTransaction(metadata, emitter -> {
                for (KafkaRecord<String, Integer> record : batch) {
                    emitter.send(KafkaRecord.of(record.getKey(), record.getPayload()));
                    processed.add(record.getPayload());
                }
                return Uni.createFrom().voidItem();
            });
        }

        public List<Integer> getProcessed() {
            return processed;
        }
    }

    @ApplicationScoped
    public static class BlockingPartitionOrderedProcessor {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        List<Integer> processed = new CopyOnWriteArrayList<>();
        Map<Integer, AtomicInteger> partitionConcurrency = new ConcurrentHashMap<>();
        Map<Integer, AtomicInteger> partitionMaxConcurrency = new ConcurrentHashMap<>();
        AtomicInteger concurrency = new AtomicInteger(0);
        AtomicInteger maxConcurrency = new AtomicInteger(0);

        @Incoming("exactly-once-consumer")
        @Blocking(ordered = false, value = "tx-pool")
        void process(ConsumerRecord<String, Integer> record, IncomingKafkaRecordMetadata<String, Integer> metadata) {
            int conc = concurrency.incrementAndGet();
            maxConcurrency.updateAndGet(max -> Math.max(max, conc));
            AtomicInteger pConc = partitionConcurrency.computeIfAbsent(record.partition(),
                    p -> new AtomicInteger(0));
            int pCurrent = pConc.incrementAndGet();
            partitionMaxConcurrency.computeIfAbsent(record.partition(), p -> new AtomicInteger(0))
                    .updateAndGet(max -> Math.max(max, pCurrent));
            transaction.withTransactionAndAwait(metadata, emitter -> {
                emitter.send(KafkaRecord.of(record.key(), record.value()));
                processed.add(record.value());
                return Uni.createFrom().voidItem().eventually(() -> {
                    pConc.decrementAndGet();
                    concurrency.decrementAndGet();
                });
            });
        }

        public List<Integer> getProcessed() {
            return processed;
        }

        public int getMaxConcurrency() {
            return maxConcurrency.get();
        }

        public Map<Integer, AtomicInteger> getPartitionMaxConcurrency() {
            return partitionMaxConcurrency;
        }

        public KafkaTransactions<Integer> transaction() {
            return transaction;
        }
    }

    @ApplicationScoped
    public static class MetadataAndAwaitProcessor {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        List<Integer> processed = new CopyOnWriteArrayList<>();

        @Incoming("exactly-once-consumer")
        @Blocking(ordered = false, value = "tx-pool")
        void process(ConsumerRecord<String, Integer> record, IncomingKafkaRecordMetadata<String, Integer> metadata) {
            transaction.withTransactionAndAwait(metadata, emitter -> {
                emitter.send(KafkaRecord.of(record.key(), record.value()));
                processed.add(record.value());
                return Uni.createFrom().voidItem();
            });
        }

        public List<Integer> getProcessed() {
            return processed;
        }
    }

    @ApplicationScoped
    public static class BatchMetadataAndAwaitProcessor {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        List<Integer> processed = new CopyOnWriteArrayList<>();

        @Incoming("exactly-once-consumer")
        @Blocking
        void process(ConsumerRecords<String, Integer> batch, IncomingKafkaRecordBatchMetadata<String, Integer> metadata) {
            transaction.withTransactionAndAwait(metadata, emitter -> {
                for (ConsumerRecord<String, Integer> record : batch) {
                    emitter.send(KafkaRecord.of(record.key(), record.value()));
                    processed.add(record.value());
                }
                return Uni.createFrom().voidItem();
            });
        }

        public List<Integer> getProcessed() {
            return processed;
        }
    }
}
