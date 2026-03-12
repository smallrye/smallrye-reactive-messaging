package io.smallrye.reactive.messaging.kafka.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaProducer;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;
import io.smallrye.reactive.messaging.kafka.impl.PooledKafkaProducer;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaProducer;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PooledKafkaProducerTest extends ClientTestBase {

    @Test
    void testPooledProducerCreation() {
        KafkaSink sink = createPooledSink();
        KafkaProducer<?, ?> producer = sink.getProducer();

        assertThat(producer).isInstanceOf(PooledKafkaProducer.class);
        assertThat(producer.isPooled()).isTrue();

        PooledKafkaProducer<?, ?> pooledProducer = (PooledKafkaProducer<?, ?>) producer;
        // Pool starts empty (lazy growth)
        assertThat(pooledProducer.getProducers()).isEmpty();

        // Using a transaction scope creates a producer in the pool
        KafkaProducer<?, ?> scope = producer.transactionScope();
        scope.beginTransaction().await().atMost(Duration.ofSeconds(10));
        assertThat(pooledProducer.getProducers()).hasSize(1);
        scope.abortTransaction().await().atMost(Duration.ofSeconds(10));

        // Producer is returned to pool, reused by next scope
        KafkaProducer<?, ?> scope2 = producer.transactionScope();
        scope2.beginTransaction().await().atMost(Duration.ofSeconds(10));
        assertThat(pooledProducer.getProducers()).hasSize(1);
        scope2.abortTransaction().await().atMost(Duration.ofSeconds(10));
    }

    @Test
    void testDistinctTransactionalAndClientIds() {
        KafkaSink sink = createPooledSink();
        KafkaProducer<?, ?> producer = sink.getProducer();
        PooledKafkaProducer<?, ?> pooledProducer = (PooledKafkaProducer<?, ?>) producer;

        // Create two concurrent scopes to force two producers in the pool
        KafkaProducer<?, ?> scope0 = producer.transactionScope();
        scope0.beginTransaction().await().atMost(Duration.ofSeconds(10));
        KafkaProducer<?, ?> scope1 = producer.transactionScope();
        scope1.beginTransaction().await().atMost(Duration.ofSeconds(10));

        assertThat(pooledProducer.getProducers()).hasSize(2);

        ReactiveKafkaProducer<?, ?> p0 = pooledProducer.getProducers().get(0);
        ReactiveKafkaProducer<?, ?> p1 = pooledProducer.getProducers().get(1);

        String txId0 = (String) p0.configuration().get(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        String txId1 = (String) p1.configuration().get(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        assertThat(txId0).isNotEqualTo(txId1);
        assertThat(txId0).endsWith("-1");
        assertThat(txId1).endsWith("-2");

        String clientId0 = (String) p0.configuration().get(ProducerConfig.CLIENT_ID_CONFIG);
        String clientId1 = (String) p1.configuration().get(ProducerConfig.CLIENT_ID_CONFIG);
        assertThat(clientId0).isNotEqualTo(clientId1);
        assertThat(clientId0).endsWith("-1");
        assertThat(clientId1).endsWith("-2");

        scope0.abortTransaction().await().atMost(Duration.ofSeconds(10));
        scope1.abortTransaction().await().atMost(Duration.ofSeconds(10));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void testSendToPartitions() {
        KafkaSink sink = createPooledSink();
        KafkaProducer producer = sink.getProducer();

        int messageCount = 10;
        ConsumerTask<Integer, String> records = companion.consume(Integer.class, String.class)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, messageCount, Duration.ofMinutes(1));

        // Send messages to specific partitions using transaction scopes
        for (int i = 0; i < messageCount; i++) {
            int partition = i % partitions;
            KafkaProducer scope = producer.transactionScope();
            scope.beginTransaction().await().atMost(Duration.ofSeconds(10));
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, partition, i, "Message " + i);
            scope.send(record).await().atMost(Duration.ofSeconds(10));
            scope.commitTransaction().await().atMost(Duration.ofSeconds(10));
        }

        records.awaitCompletion(Duration.ofMinutes(1));
        assertThat(records.count()).isEqualTo(messageCount);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void testDirectSendThrows() {
        KafkaSink sink = createPooledSink();
        KafkaProducer producer = sink.getProducer();

        ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, 0, 1, "Message 1");
        assertThatThrownBy(() -> producer.send(record))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testCloseClosesAllProducers() {
        KafkaSink sink = createPooledSink();
        KafkaProducer<?, ?> producer = sink.getProducer();
        PooledKafkaProducer<?, ?> pooledProducer = (PooledKafkaProducer<?, ?>) producer;

        // Create producers by using scopes
        KafkaProducer<?, ?> scope0 = producer.transactionScope();
        scope0.beginTransaction().await().atMost(Duration.ofSeconds(10));
        KafkaProducer<?, ?> scope1 = producer.transactionScope();
        scope1.beginTransaction().await().atMost(Duration.ofSeconds(10));
        scope0.abortTransaction().await().atMost(Duration.ofSeconds(10));
        scope1.abortTransaction().await().atMost(Duration.ofSeconds(10));

        assertThat(pooledProducer.getProducers()).hasSize(2);

        // Close
        producer.close();
        assertThat(producer.isClosed()).isTrue();
        assertThat(pooledProducer.getProducers()).isEmpty();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void testTransactionLifecycleBeginCommit() {
        KafkaSink sink = createPooledSink();
        KafkaProducer producer = sink.getProducer();
        PooledKafkaProducer pooledProducer = (PooledKafkaProducer) producer;

        int messageCount = 6;
        ConsumerTask<Integer, String> records = companion.consume(Integer.class, String.class)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, messageCount, Duration.ofMinutes(1));

        // Begin a transaction scope, send to multiple partitions, then commit
        KafkaProducer scope = producer.transactionScope();
        scope.beginTransaction().await().atMost(Duration.ofSeconds(10));

        for (int i = 0; i < messageCount; i++) {
            int partition = i % partitions;
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, partition, i, "Message " + i);
            scope.send(record).await().atMost(Duration.ofSeconds(10));
        }

        scope.commitTransaction().await().atMost(Duration.ofSeconds(10));

        records.awaitCompletion(Duration.ofMinutes(1));
        assertThat(records.count()).isEqualTo(messageCount);
        // A single producer handles all partitions within one scope
        assertThat(pooledProducer.getProducers()).hasSize(1);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void testTransactionLifecycleBeginAbort() {
        KafkaSink sink = createPooledSink();
        KafkaProducer producer = sink.getProducer();

        // Begin a scope, send records, then abort — records should not be visible
        KafkaProducer scope1 = producer.transactionScope();
        scope1.beginTransaction().await().atMost(Duration.ofSeconds(10));

        for (int i = 0; i < 3; i++) {
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, i % partitions, i, "Aborted " + i);
            scope1.send(record).await().atMost(Duration.ofSeconds(10));
        }

        scope1.abortTransaction().await().atMost(Duration.ofSeconds(10));

        // Now send committed records via a new scope
        int committedCount = 3;
        ConsumerTask<Integer, String> records = companion.consume(Integer.class, String.class)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, committedCount, Duration.ofMinutes(1));

        KafkaProducer scope2 = producer.transactionScope();
        scope2.beginTransaction().await().atMost(Duration.ofSeconds(10));
        for (int i = 0; i < committedCount; i++) {
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, i % partitions, i, "Committed " + i);
            scope2.send(record).await().atMost(Duration.ofSeconds(10));
        }
        scope2.commitTransaction().await().atMost(Duration.ofSeconds(10));

        records.awaitCompletion(Duration.ofMinutes(1));
        // Only committed records should be visible
        assertThat(records.count()).isEqualTo(committedCount);
        assertThat(records.getRecords()).allSatisfy(r -> assertThat(r.value()).startsWith("Committed"));
    }

    @Test
    void testGlobalTransactionOperationsThrow() {
        KafkaSink sink = createPooledSink();
        KafkaProducer<?, ?> producer = sink.getProducer();

        assertThatThrownBy(() -> producer.beginTransaction().await().indefinitely())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> producer.commitTransaction().await().indefinitely())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> producer.abortTransaction().await().indefinitely())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testUnwrapReturnsNullWhenPoolEmpty() {
        KafkaSink sink = createPooledSink();
        KafkaProducer<?, ?> producer = sink.getProducer();

        // Pool is empty at start — unwrap returns null
        assertThat(producer.unwrap()).isNull();
    }

    @Test
    void testConfigurationReturnsBaseConfig() {
        KafkaSink sink = createPooledSink();
        KafkaProducer<?, ?> producer = sink.getProducer();

        Map<String, ?> config = producer.configuration();
        assertThat(config).isNotNull();
        assertThat(config.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).isNotNull();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void testConcurrentTransactionScopes() {
        KafkaSink sink = createPooledSink();
        KafkaProducer producer = sink.getProducer();

        int perScope = 3;
        int total = perScope * 2;
        ConsumerTask<Integer, String> records = companion.consume(Integer.class, String.class)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, total, Duration.ofMinutes(1));

        // Create two scopes concurrently
        KafkaProducer scope0 = producer.transactionScope();
        KafkaProducer scope1 = producer.transactionScope();
        assertThat(scope0).isNotSameAs(scope1);

        scope0.beginTransaction().await().atMost(Duration.ofSeconds(10));
        scope1.beginTransaction().await().atMost(Duration.ofSeconds(10));

        for (int i = 0; i < perScope; i++) {
            scope0.send(new ProducerRecord<>(topic, 0, i, "Scope0-" + i)).await().atMost(Duration.ofSeconds(10));
            scope1.send(new ProducerRecord<>(topic, 1, i + perScope, "Scope1-" + i)).await().atMost(Duration.ofSeconds(10));
        }

        // Commit both scopes independently
        CompletableFuture<Void> commit0 = scope0.commitTransaction().subscribeAsCompletionStage();
        CompletableFuture<Void> commit1 = scope1.commitTransaction().subscribeAsCompletionStage();
        commit0.join();
        commit1.join();

        records.awaitCompletion(Duration.ofMinutes(1));
        assertThat(records.count()).isEqualTo(total);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void testTransactionScopeSendToMultiplePartitions() {
        KafkaSink sink = createPooledSink();
        KafkaProducer producer = sink.getProducer();
        PooledKafkaProducer pooledProducer = (PooledKafkaProducer) producer;

        int messageCount = 8;
        ConsumerTask<Integer, String> records = companion.consume(Integer.class, String.class)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, messageCount, Duration.ofMinutes(1));

        // A single scope sends to multiple output partitions — all via one producer
        KafkaProducer scope = producer.transactionScope();
        scope.beginTransaction().await().atMost(Duration.ofSeconds(10));

        for (int i = 0; i < messageCount; i++) {
            int partition = i % partitions;
            scope.send(new ProducerRecord<>(topic, partition, i, "Multi-" + i)).await().atMost(Duration.ofSeconds(10));
        }

        scope.commitTransaction().await().atMost(Duration.ofSeconds(10));

        records.awaitCompletion(Duration.ofMinutes(1));
        assertThat(records.count()).isEqualTo(messageCount);
        // Only one inner producer used — single atomic transaction
        assertThat(pooledProducer.getProducers()).hasSize(1);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void testTransactionScopeFlush() {
        KafkaSink sink = createPooledSink();
        KafkaProducer producer = sink.getProducer();

        int messageCount = 4;
        ConsumerTask<Integer, String> records = companion.consume(Integer.class, String.class)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, messageCount, Duration.ofMinutes(1));

        KafkaProducer scope = producer.transactionScope();
        scope.beginTransaction().await().atMost(Duration.ofSeconds(10));

        for (int i = 0; i < messageCount; i++) {
            scope.send(new ProducerRecord<>(topic, i % partitions, i, "Flush-" + i)).await().atMost(Duration.ofSeconds(10));
        }

        // Flush before commit — ensures buffered records are sent
        scope.flush().await().atMost(Duration.ofSeconds(10));
        scope.commitTransaction().await().atMost(Duration.ofSeconds(10));

        records.awaitCompletion(Duration.ofMinutes(1));
        assertThat(records.count()).isEqualTo(messageCount);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void testSendWithoutPartitionAllowed() {
        KafkaSink sink = createPooledSink();
        KafkaProducer producer = sink.getProducer();

        int messageCount = 3;
        ConsumerTask<Integer, String> records = companion.consume(Integer.class, String.class)
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, messageCount, Duration.ofMinutes(1));

        // Sending without explicit partition is allowed — Kafka partitioner decides
        KafkaProducer scope = producer.transactionScope();
        scope.beginTransaction().await().atMost(Duration.ofSeconds(10));
        for (int i = 0; i < messageCount; i++) {
            scope.send(new ProducerRecord<>(topic, null, i, "NoPartition-" + i))
                    .await().atMost(Duration.ofSeconds(10));
        }
        scope.commitTransaction().await().atMost(Duration.ofSeconds(10));

        records.awaitCompletion(Duration.ofMinutes(1));
        assertThat(records.count()).isEqualTo(messageCount);
    }

    @Test
    void testTransactionScopeProperties() {
        KafkaSink sink = createPooledSink();
        KafkaProducer<?, ?> producer = sink.getProducer();

        KafkaProducer<?, ?> scope = producer.transactionScope();
        assertThat(scope.isPooled()).isTrue();
        assertThat(scope.isClosed()).isFalse();
        assertThat(scope.configuration()).isEqualTo(producer.configuration());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void testPoolReusesProducers() {
        KafkaSink sink = createPooledSink();
        KafkaProducer producer = sink.getProducer();
        PooledKafkaProducer pooledProducer = (PooledKafkaProducer) producer;

        // First scope creates a new producer
        KafkaProducer scope1 = producer.transactionScope();
        scope1.beginTransaction().await().atMost(Duration.ofSeconds(10));
        scope1.send(new ProducerRecord<>(topic, 0, 1, "msg1")).await().atMost(Duration.ofSeconds(10));
        scope1.commitTransaction().await().atMost(Duration.ofSeconds(10));
        assertThat(pooledProducer.getProducers()).hasSize(1);

        // Second scope reuses the same producer (it was released back to pool)
        KafkaProducer scope2 = producer.transactionScope();
        scope2.beginTransaction().await().atMost(Duration.ofSeconds(10));
        scope2.send(new ProducerRecord<>(topic, 0, 2, "msg2")).await().atMost(Duration.ofSeconds(10));
        scope2.commitTransaction().await().atMost(Duration.ofSeconds(10));
        // Still only 1 producer in the pool
        assertThat(pooledProducer.getProducers()).hasSize(1);
    }

    @Test
    void testInitialPoolSizePreCreatesProducers() {
        int initialSize = 3;
        KafkaSink sink = createPooledSink(initialSize, 10);
        KafkaProducer<?, ?> producer = sink.getProducer();
        PooledKafkaProducer<?, ?> pooledProducer = (PooledKafkaProducer<?, ?>) producer;

        // Initial producers should be pre-created
        assertThat(pooledProducer.getProducers()).hasSize(initialSize);

        // They should be usable via transaction scopes
        KafkaProducer<?, ?> scope = producer.transactionScope();
        scope.beginTransaction().await().atMost(Duration.ofSeconds(10));
        // Should reuse one of the pre-created producers, not create a new one
        assertThat(pooledProducer.getProducers()).hasSize(initialSize);
        scope.abortTransaction().await().atMost(Duration.ofSeconds(10));
    }

    @Test
    void testPoolNeverExceedsMaxSize() {
        int maxSize = 2;
        KafkaSink sink = createPooledSink(0, maxSize);
        KafkaProducer<?, ?> producer = sink.getProducer();
        PooledKafkaProducer<?, ?> pooledProducer = (PooledKafkaProducer<?, ?>) producer;

        // Create maxSize concurrent scopes to fill up the pool
        KafkaProducer<?, ?> scope0 = producer.transactionScope();
        scope0.beginTransaction().await().atMost(Duration.ofSeconds(10));
        KafkaProducer<?, ?> scope1 = producer.transactionScope();
        scope1.beginTransaction().await().atMost(Duration.ofSeconds(10));

        assertThat(pooledProducer.getProducers()).hasSize(maxSize);

        // Release one and create another — pool should still be at maxSize
        scope0.abortTransaction().await().atMost(Duration.ofSeconds(10));

        KafkaProducer<?, ?> scope2 = producer.transactionScope();
        scope2.beginTransaction().await().atMost(Duration.ofSeconds(10));
        // Should reuse the released producer, not exceed max
        assertThat(pooledProducer.getProducers()).hasSize(maxSize);

        scope1.abortTransaction().await().atMost(Duration.ofSeconds(10));
        scope2.abortTransaction().await().atMost(Duration.ofSeconds(10));
    }

    @Test
    void testAcquireThrowsWhenPoolExhausted() {
        int maxSize = 1;
        KafkaSink sink = createPooledSink(0, maxSize);
        KafkaProducer<?, ?> producer = sink.getProducer();

        // Exhaust the pool
        KafkaProducer<?, ?> scope0 = producer.transactionScope();
        scope0.beginTransaction().await().atMost(Duration.ofSeconds(10));

        // A second scope should fail immediately
        KafkaProducer<?, ?> scope1 = producer.transactionScope();
        assertThatThrownBy(() -> scope1.beginTransaction().await().atMost(Duration.ofSeconds(10)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pool exhausted");

        scope0.abortTransaction().await().atMost(Duration.ofSeconds(10));
    }

    private KafkaSink createPooledSink() {
        return createPooledSink(0, 10);
    }

    private KafkaSink createPooledSink(int initialPoolSize, int maxPoolSize) {
        String channelName = "test-" + ThreadLocalRandom.current().nextInt();
        MapBasedConfig config = createProducerConfig()
                .with("channel-name", channelName)
                .with(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-producer")
                .with(ProducerConfig.ACKS_CONFIG, "all")
                .with("pooled-producer", true)
                .with("pooled-producer.initial-pool-size", initialPoolSize)
                .with("pooled-producer.max-pool-size", maxPoolSize)
                .with("topic", topic);

        KafkaSink sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config), CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), UnsatisfiedInstance.instance(), UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance());
        this.sinks.add(sink);
        return sink;
    }
}
