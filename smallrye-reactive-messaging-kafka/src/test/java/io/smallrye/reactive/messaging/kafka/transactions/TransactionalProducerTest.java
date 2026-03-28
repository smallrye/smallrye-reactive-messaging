package io.smallrye.reactive.messaging.kafka.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TransactionAbortedException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.PerfTestUtils;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.Vertx;
import io.vertx.mutiny.core.Context;

public class TransactionalProducerTest extends KafkaCompanionTestBase {

    private KafkaMapBasedConfig config() {
        return kafkaConfig("mp.messaging.outgoing.transactional-producer")
                .put("topic", topic)
                .put("transactional.id", "tx-producer")
                .put("acks", "all")
                .put("key.serializer", StringSerializer.class.getName())
                .put("value.serializer", IntegerSerializer.class.getName());
    }

    private KafkaMapBasedConfig pooledConfig() {
        return config().put("pooled-producer", true);
    }

    @Test
    void testTransactionInCallerThread() {
        topic = companion.topics().createAndWait(topic, 3);
        int numberOfRecords = 100;
        TransactionalProducer application = runApplication(config(), TransactionalProducer.class);

        application.produceInTransaction(numberOfRecords).await().indefinitely();
        assertThat(application.transaction().isTransactionInProgress()).isFalse();

        companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, numberOfRecords)
                .awaitCompletion(Duration.ofMinutes(1));
    }

    @Test
    void testTransactionFromVertxContext() {
        topic = companion.topics().createAndWait(topic, 3);
        int numberOfRecords = 100;
        TransactionalProducerEventLoop application = runApplication(config(), TransactionalProducerEventLoop.class);

        Uni.createFrom().emitter(e -> {
            application.produceInTransaction(numberOfRecords)
                    .invoke(() -> {
                        assertThat(Vertx.currentContext()).isNotNull();
                        assertThat(Context.isOnEventLoopThread()).isTrue();
                    })
                    .subscribe().with(unused -> e.complete(null), e::fail);
        }).runSubscriptionOn(runnable -> vertx.runOnContext(runnable))
                .await().indefinitely();

        companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, numberOfRecords)
                .awaitCompletion(Duration.ofMinutes(1));
    }

    @Test
    // TODO this used to pass in its reactive form on Vert.x 4
    void testTransactionFromVertxContextBlocking() {
        topic = companion.topics().createAndWait(topic, 3);
        int numberOfRecords = 100;
        TransactionalProducerBlocking application = runApplication(config(), TransactionalProducerBlocking.class);

        vertx.executeBlocking(() -> {
            application.produceInTransaction(numberOfRecords);
            return null;
        }).await().indefinitely();

        companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, numberOfRecords)
                .awaitCompletion(Duration.ofMinutes(1));
    }

    @Test
    void testPooledTransactionConcurrent() {
        topic = companion.topics().createAndWait(topic, 3);
        int numberOfRecords = 10;
        TransactionalProducer application = runApplication(pooledConfig(), TransactionalProducer.class);

        // Run two transactions concurrently — both should succeed with a pooled producer
        Uni<Void> tx1 = application.produceInTransaction(numberOfRecords);
        Uni<Void> tx2 = application.produceInTransaction(numberOfRecords);

        Uni.join().all(tx1, tx2).andCollectFailures()
                .await().atMost(Duration.ofMinutes(1));
        assertThat(application.transaction().isTransactionInProgress()).isFalse();

        companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, numberOfRecords * 2)
                .awaitCompletion(Duration.ofMinutes(1));
    }

    @ApplicationScoped
    public static class TransactionalProducer {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        Uni<Void> produceInTransaction(final int numberOfRecords) {
            return transaction.withTransaction(emitter -> {
                for (int i = 0; i < numberOfRecords; i++) {
                    emitter.send(KafkaRecord.of("" + i % 10, i));
                }
                assertThat(transaction.isTransactionInProgress()).isTrue();
                return Uni.createFrom().voidItem();
            });
        }

        public KafkaTransactions<Integer> transaction() {
            return transaction;
        }
    }

    @ApplicationScoped
    public static class TransactionalProducerEventLoop {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        Uni<Void> produceInTransaction(final int numberOfRecords) {
            return transaction.withTransaction(emitter -> {
                assertThat(Vertx.currentContext()).isNotNull();
                assertThat(Context.isOnEventLoopThread()).isTrue();
                for (int i = 0; i < numberOfRecords; i++) {
                    emitter.send(KafkaRecord.of("" + i % 10, i));
                }
                assertThat(transaction.isTransactionInProgress()).isTrue();
                return Uni.createFrom().voidItem();
            });
        }
    }

    @ApplicationScoped
    public static class TransactionalProducerBlocking {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        void produceInTransaction(final int numberOfRecords) {
            assertThat(Vertx.currentContext()).isNotNull();
            assertThat(Context.isOnWorkerThread()).isTrue();
//            return transaction.withTransaction(emitter -> {
            transaction.withTransactionAndAwait(emitter -> {
                assertThat(Vertx.currentContext()).isNotNull();
                assertThat(Context.isOnWorkerThread()).isTrue();
                for (int i = 0; i < numberOfRecords; i++) {
                    emitter.send(KafkaRecord.of("" + i % 10, i));
                }
                assertThat(transaction.isTransactionInProgress()).isTrue();
                return Uni.createFrom().voidItem();
            });
        }
    }

    @Test
    void testConcurrentCallsNotAllowed() {
        topic = companion.topics().createAndWait(topic, 3);
        int numberOfRecords = 100;
        TransactionalProducerWithNestedCall application = runApplication(config(), TransactionalProducerWithNestedCall.class);

        assertThatThrownBy(() -> {
            Uni.createFrom().emitter(e -> {
                application.produceInTransaction(numberOfRecords)
                        .subscribe().with(unused -> e.complete(null), e::fail);
            }).runSubscriptionOn(runnable -> vertx.runOnContext(runnable))
                    .await().indefinitely();
        }).isInstanceOf(IllegalStateException.class).hasMessageContaining("transactional-producer");
        assertThat(application.transaction().isTransactionInProgress()).isFalse();

        assertThat(companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, Duration.ofSeconds(5))
                .awaitCompletion()
                .count()).isZero();
    }

    @ApplicationScoped
    public static class TransactionalProducerWithNestedCall {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        Uni<Void> produceInTransaction(final int numberOfRecords) {
            return transaction.withTransaction(emitter -> {
                for (int i = 0; i < numberOfRecords; i++) {
                    emitter.send(KafkaRecord.of("" + i % 10, i));
                }
                return transaction.withTransaction(e -> Uni.createFrom().voidItem());
            });
        }

        public KafkaTransactions<Integer> transaction() {
            return transaction;
        }
    }

    @Test
    void testFailingTransactionalProducer() {
        topic = companion.topics().createAndWait(topic, 3);
        int numberOfRecords = 100;
        FailingTransactionalProducer application = runApplication(config(), FailingTransactionalProducer.class);

        Assertions.assertThatThrownBy(() -> {
            application.produceInTransaction(numberOfRecords,
                    (e) -> Uni.createFrom().failure(new IllegalStateException("boom")))
                    .await().indefinitely();
        }).isInstanceOf(IllegalStateException.class);
        assertThat(application.transaction().isTransactionInProgress()).isFalse();

        assertThat(companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, Duration.ofSeconds(5))
                .awaitCompletion()
                .count()).isZero();
    }

    @Test
    void testAbortingTransactionalProducer() {
        topic = companion.topics().createAndWait(topic, 3);
        int numberOfRecords = 100;
        FailingTransactionalProducer application = runApplication(config(), FailingTransactionalProducer.class);

        Assertions.assertThatThrownBy(() -> {
            application.produceInTransaction(numberOfRecords, (e) -> {
                if (!e.isMarkedForAbort()) {
                    e.markForAbort();
                }
                return Uni.createFrom().voidItem();
            }).await().indefinitely();
        }).isInstanceOf(TransactionAbortedException.class);
        assertThat(application.transaction().isTransactionInProgress()).isFalse();

        assertThat(companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, Duration.ofSeconds(5))
                .awaitCompletion()
                .count()).isZero();
    }

    @ApplicationScoped
    public static class FailingTransactionalProducer {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        Uni<Void> produceInTransaction(final int numberOfRecords,
                Function<TransactionalEmitter<Integer>, Uni<Void>> failingSupplier) {
            return transaction.withTransaction(emitter -> {
                for (int i = 0; i < numberOfRecords; i++) {
                    emitter.send(i);
                }
                return failingSupplier.apply(emitter);
            });
        }

        public KafkaTransactions<Integer> transaction() {
            return transaction;
        }
    }

    @Test
    void testRetryingTransactionalProducer() {
        topic = companion.topics().createAndWait(topic, 3);
        int numberOfRecords = 100;
        RetryingTransactionalProducer application = runApplication(config(), RetryingTransactionalProducer.class);

        application.produceInTransaction(numberOfRecords)
                .onFailure().retry().atMost(3)
                .await().indefinitely();
        assertThat(application.getRetries()).isEqualTo(3);

        companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, numberOfRecords)
                .awaitCompletion();
    }

    @ApplicationScoped
    public static class RetryingTransactionalProducer {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        AtomicInteger retries = new AtomicInteger();

        Uni<Void> produceInTransaction(final int numberOfRecords) {
            return transaction.withTransaction(emitter -> {
                for (int i = 0; i < numberOfRecords; i++) {
                    emitter.send(KafkaRecord.of("" + i % 10, i));
                }
                int attempt = retries.incrementAndGet();
                if (attempt < 3) {
                    throw new IllegalStateException("try " + attempt);
                }
                return Uni.createFrom().voidItem();
            });
        }

        public int getRetries() {
            return retries.get();
        }
    }

    @Test
    void testTransactionalConsumer() {
        String inTopic = companion.topics().createAndWait(UUID.randomUUID().toString(), 1);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(inTopic, "v-" + i), 10)
                .awaitCompletion();

        KafkaMapBasedConfig inConfig = kafkaConfig("mp.messaging.incoming.in", false)
                .with("topic", inTopic)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName());

        KafkaMapBasedConfig outconfig = config();
        MapBasedConfig config = new MapBasedConfig(outconfig.getMap());
        config.putAll(inConfig.getMap());

        runApplication(config, TransactionalProducerFromIncoming.class);

        companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, 30)
                .awaitCompletion(Duration.ofMinutes(1));
    }

    @Test
    // TODO this used to pass in its reactive form on Vert.x 4
    void testTransactionalConsumerBlocking() {
        String inTopic = companion.topics().createAndWait(UUID.randomUUID().toString(), 1);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(inTopic, "v-" + i), 10)
                .awaitCompletion();

        KafkaMapBasedConfig inConfig = kafkaConfig("mp.messaging.incoming.in", false)
                .with("topic", inTopic)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName());

        KafkaMapBasedConfig outconfig = config();
        MapBasedConfig config = new MapBasedConfig(outconfig.getMap());
        config.putAll(inConfig.getMap());

        runApplication(config, TransactionalProducerFromIncomingBlocking.class);

        companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, 30)
                .awaitCompletion(Duration.ofMinutes(1));
    }

    @ApplicationScoped
    public static class TransactionalProducerFromIncoming {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        @Incoming("in")
        Uni<Void> produceInTransaction(String msg) {
            return transaction.withTransaction(emitter -> {
                emitter.send(KafkaRecord.of(msg, 1));
                emitter.send(KafkaRecord.of(msg, 2));
                emitter.send(KafkaRecord.of(msg, 3));
                return Uni.createFrom().voidItem();
            });
        }

    }

    @ApplicationScoped
    public static class TransactionalProducerFromIncomingBlocking {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        @Incoming("in")
        @Blocking
        void produceInTransaction(String msg) {
            transaction.withTransactionAndAwait(emitter -> {
                assertThat(Vertx.currentContext()).isNotNull();
                assertThat(Context.isOnWorkerThread()).isTrue();
                emitter.send(KafkaRecord.of(msg, 1));
                emitter.send(KafkaRecord.of(msg, 2));
                emitter.send(KafkaRecord.of(msg, 3));
                return Uni.createFrom().voidItem();
            });
//            return transaction.withTransaction(emitter -> {
//                assertThat(Vertx.currentContext()).isNotNull();
//                assertThat(Context.isOnWorkerThread()).isTrue();
//                emitter.send(KafkaRecord.of(msg, 1));
//                emitter.send(KafkaRecord.of(msg, 2));
//                emitter.send(KafkaRecord.of(msg, 3));
//                return Uni.createFrom().voidItem();
//            });
        }

    }

    @Test
    void testWithTransactionAndAwait() {
        topic = companion.topics().createAndWait(topic, 3);
        int numberOfRecords = 10;
        TransactionalProducerAndAwait application = runApplication(config(), TransactionalProducerAndAwait.class);

        application.produceInTransactionBlocking(numberOfRecords);
        assertThat(application.transaction().isTransactionInProgress()).isFalse();

        companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, numberOfRecords)
                .awaitCompletion(Duration.ofMinutes(1));
    }

    @ApplicationScoped
    public static class TransactionalProducerAndAwait {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<Integer> transaction;

        void produceInTransactionBlocking(final int numberOfRecords) {
            transaction.withTransactionAndAwait(emitter -> {
                for (int i = 0; i < numberOfRecords; i++) {
                    emitter.send(KafkaRecord.of("" + i % 10, i));
                }
                return Uni.createFrom().voidItem();
            });
        }

        public KafkaTransactions<Integer> transaction() {
            return transaction;
        }
    }

    @Test
    void testFailingSendInTransaction() {
        topic = companion.topics().createAndWait(topic, 3);
        int numberOfRecords = 100;
        BigTransactionalProducer application = runApplication(config()
                .with("value.serializer", ByteArraySerializer.class.getName())
                .with("max.request.size", 100), BigTransactionalProducer.class);

        assertThatThrownBy(() -> application.produceInTransaction(numberOfRecords).await().indefinitely())
                .isInstanceOf(CompositeException.class)
                .hasCauseExactlyInstanceOf(RecordTooLargeException.class);
        assertThat(application.transaction().isTransactionInProgress()).isFalse();

        companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, numberOfRecords)
                .awaitNoRecords(Duration.ofSeconds(3));
    }

    @ApplicationScoped
    public static class BigTransactionalProducer {

        @Inject
        @Channel("transactional-producer")
        KafkaTransactions<byte[]> transaction;

        Uni<Void> produceInTransaction(final int numberOfRecords) {
            return transaction.withTransaction(emitter -> {
                for (int i = 0; i < numberOfRecords; i++) {
                    emitter.send(KafkaRecord.of("" + i % 10, PerfTestUtils.generateRandomPayload(numberOfRecords)));
                }
                assertThat(transaction.isTransactionInProgress()).isTrue();
                return Uni.createFrom().voidItem();
            });
        }

        public KafkaTransactions<byte[]> transaction() {
            return transaction;
        }
    }
}
