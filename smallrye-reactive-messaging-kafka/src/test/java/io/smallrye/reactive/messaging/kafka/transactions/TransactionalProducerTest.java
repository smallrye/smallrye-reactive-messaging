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
import org.apache.kafka.common.errors.TransactionAbortedException;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class TransactionalProducerTest extends KafkaCompanionTestBase {

    private KafkaMapBasedConfig config() {
        return kafkaConfig("mp.messaging.outgoing.transactional-producer")
                .put("topic", topic)
                .put("transactional.id", "tx-producer")
                .put("acks", "all")
                .put("key.serializer", StringSerializer.class.getName())
                .put("value.serializer", IntegerSerializer.class.getName());
    }

    @Test
    void testTransactionInCallerThread() {
        topic = companion.topics().createAndWait(topic, 3);
        int numberOfRecords = 100;
        TransactionalProducer application = runApplication(config(), TransactionalProducer.class);

        application.produceInTransaction(numberOfRecords).await().indefinitely();

        companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, numberOfRecords)
                .awaitCompletion(Duration.ofMinutes(1));
    }

    @Test
    void testTransactionFromVertxContext() {
        topic = companion.topics().createAndWait(topic, 3);
        int numberOfRecords = 100;
        TransactionalProducer application = runApplication(config(), TransactionalProducer.class);

        Uni.createFrom().emitter(e -> {
            application.produceInTransaction(numberOfRecords)
                    .subscribe().with(unused -> e.complete(null), e::fail);
        }).runSubscriptionOn(runnable -> vertx.runOnContext(runnable))
                .await().indefinitely();

        companion.consumeIntegers()
                .withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .fromTopics(topic, numberOfRecords)
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
}
