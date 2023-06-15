package io.smallrye.reactive.messaging.pulsar.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.PulsarMessage;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class TransactionalProducerTest extends WeldTestBase {

    private MapBasedConfig config() {
        return baseConfig()
                .with("mp.messaging.outgoing.transactional-producer.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.transactional-producer.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.transactional-producer.enableTransaction", true)
                .with("mp.messaging.outgoing.transactional-producer.topic", topic)
                .with("mp.messaging.outgoing.transactional-producer.schema", "STRING");
    }

    private MapBasedConfig configInt() {
        return baseConfig()
                .with("mp.messaging.outgoing.transactional-producer-int.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.transactional-producer-int.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.transactional-producer-int.enableTransaction", true)
                .with("mp.messaging.outgoing.transactional-producer-int.topic", topic + "-int")
                .with("mp.messaging.outgoing.transactional-producer-int.schema", "INT32");
    }

    @Test
    void testTransactionInCallerThread() throws PulsarAdminException, PulsarClientException {
        admin.topics().createPartitionedTopic(topic, 3);
        int numberOfRecords = 100;
        TransactionalProducer application = runApplication(config()
                .with("mp.messaging.outgoing.transactional-producer.schema", "INT32"), TransactionalProducer.class);

        application.produceInTransaction(numberOfRecords).await().indefinitely();

        List<Integer> received = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .consumerName("test-consumer")
                .subscriptionName("test-subscription")
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe(), numberOfRecords, integerMessage -> received.add(integerMessage.getValue()));
        await().untilAsserted(() -> assertThat(received).hasSize(100));
    }

    @Test
    void testTransactionFromVertxContext() throws PulsarAdminException, PulsarClientException {
        admin.topics().createPartitionedTopic(topic, 3);
        int numberOfRecords = 100;
        TransactionalProducer application = runApplication(config()
                .with("mp.messaging.outgoing.transactional-producer.schema", "INT32"), TransactionalProducer.class);

        Uni.createFrom().emitter(e -> {
            application.produceInTransaction(numberOfRecords)
                    .subscribe().with(unused -> e.complete(null), e::fail);
        }).runSubscriptionOn(runnable -> vertx.runOnContext(runnable))
                .await().indefinitely();

        List<Integer> received = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .consumerName("test-consumer")
                .subscriptionName("test-subscription")
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe(), numberOfRecords, integerMessage -> received.add(integerMessage.getValue()));
        await().untilAsserted(() -> assertThat(received).hasSize(100));
    }

    @ApplicationScoped
    public static class TransactionalProducer {

        @Inject
        @Channel("transactional-producer")
        PulsarTransactions<Integer> transaction;

        Uni<Void> produceInTransaction(final int numberOfRecords) {
            return transaction.withTransaction(emitter -> {
                for (int i = 0; i < numberOfRecords; i++) {
                    emitter.send(PulsarMessage.of(i, String.valueOf(i % 10)));
                }
                assertThat(transaction.isTransactionInProgress()).isTrue();
                return Uni.createFrom().voidItem();
            });
        }
    }

    @Test
    //    @Disabled
    void testConcurrentCallsAreAllowed() throws PulsarAdminException, PulsarClientException {
        admin.topics().createPartitionedTopic(topic, 3);
        int numberOfRecords = 100;
        TransactionalProducerWithNestedCall application = runApplication(config()
                .with("mp.messaging.outgoing.transactional-producer.schema", "INT32"),
                TransactionalProducerWithNestedCall.class);

        Uni.createFrom().emitter(e -> {
            application.produceInTransaction(numberOfRecords)
                    .subscribe().with(unused -> e.complete(null), e::fail);
        }).runSubscriptionOn(runnable -> vertx.runOnContext(runnable))
                .await().indefinitely();

        List<Integer> received = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .consumerName("test-consumer")
                .subscriptionName("test-subscription")
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe(), numberOfRecords, integerMessage -> received.add(integerMessage.getValue()));
        await().untilAsserted(() -> assertThat(received).hasSize(100));
    }

    @ApplicationScoped
    public static class TransactionalProducerWithNestedCall {

        @Inject
        @Channel("transactional-producer")
        PulsarTransactions<Integer> transaction;

        Uni<Void> produceInTransaction(final int numberOfRecords) {
            return transaction.withTransaction(emitter -> {
                for (int i = 0; i < numberOfRecords; i++) {
                    emitter.send(PulsarMessage.of(i, String.valueOf(i % 10)));
                }
                return transaction.withTransaction(e -> Uni.createFrom().voidItem());
            });
        }
    }

    @Test
    void testFailingTransactionalProducer() throws PulsarAdminException, PulsarClientException {
        admin.topics().createPartitionedTopic(topic, 3);
        int numberOfRecords = 100;
        FailingTransactionalProducer application = runApplication(config()
                .with("mp.messaging.outgoing.transactional-producer.schema", "INT32"), FailingTransactionalProducer.class);

        Assertions.assertThatThrownBy(() -> {
            application.produceInTransaction(numberOfRecords,
                    (e) -> Uni.createFrom().failure(new IllegalStateException("boom")))
                    .await().indefinitely();
        }).isInstanceOf(IllegalStateException.class);

        List<Integer> received = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .consumerName("test-consumer")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("test-subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic(topic)
                .subscribe(), numberOfRecords, integerMessage -> received.add(integerMessage.getValue()));
        await().pollDelay(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).hasSize(0));
    }

    @Test
    void testAbortingTransactionalProducer() throws PulsarAdminException, PulsarClientException {
        admin.topics().createPartitionedTopic(topic, 3);
        int numberOfRecords = 100;
        FailingTransactionalProducer application = runApplication(config()
                .with("mp.messaging.outgoing.transactional-producer.schema", "INT32"), FailingTransactionalProducer.class);

        Assertions.assertThatThrownBy(() -> {
            application.produceInTransaction(numberOfRecords, (e) -> {
                if (!e.isMarkedForAbort()) {
                    e.markForAbort();
                }
                return Uni.createFrom().voidItem();
            }).await().indefinitely();
        }).isInstanceOf(RuntimeException.class);

        List<Integer> received = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .subscriptionType(SubscriptionType.Shared)
                .consumerName("test-consumer")
                .subscriptionName("test-subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic(topic)
                .subscribe(), numberOfRecords, integerMessage -> received.add(integerMessage.getValue()));

        await().pollDelay(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).hasSize(0));
    }

    @ApplicationScoped
    public static class FailingTransactionalProducer {

        @Inject
        @Channel("transactional-producer")
        PulsarTransactions<Integer> transaction;

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
    void testRetryingTransactionalProducer() throws PulsarAdminException, PulsarClientException {
        admin.topics().createPartitionedTopic(topic, 3);
        int numberOfRecords = 100;
        RetryingTransactionalProducer application = runApplication(config()
                .with("mp.messaging.outgoing.transactional-producer.schema", "INT32"), RetryingTransactionalProducer.class);

        application.produceInTransaction(numberOfRecords)
                .onFailure().retry().atMost(3)
                .await().indefinitely();
        assertThat(application.getRetries()).isEqualTo(3);

        List<Integer> received = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .consumerName("test-consumer")
                .subscriptionName("test-subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic(topic)
                .subscribe(), numberOfRecords, integerMessage -> received.add(integerMessage.getValue()));
        await().untilAsserted(() -> assertThat(received).hasSize(100));
    }

    @ApplicationScoped
    public static class RetryingTransactionalProducer {

        @Inject
        @Channel("transactional-producer")
        PulsarTransactions<Integer> transaction;

        AtomicInteger retries = new AtomicInteger();

        Uni<Void> produceInTransaction(final int numberOfRecords) {
            return transaction.withTransaction(emitter -> {
                for (int i = 0; i < numberOfRecords; i++) {
                    emitter.send(PulsarMessage.of(i, String.valueOf(i % 10)));
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
    void testTransactionalConsumer() throws PulsarAdminException, PulsarClientException {
        String inTopic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(inTopic, 3);

        send(client.newProducer(Schema.STRING)
                .producerName("producer")
                .topic(inTopic)
                .create(), 10, (i, producer) -> producer.newMessage().value("v-" + i));

        MapBasedConfig inConfig = baseConfig()
                .with("mp.messaging.incoming.in.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.in.topic", inTopic)
                .with("mp.messaging.incoming.in.subscriptionInitialPosition", "Earliest")
                .with("mp.messaging.incoming.in.schema", "STRING");

        MapBasedConfig outconfig = config();
        MapBasedConfig config = new MapBasedConfig(outconfig.getMap());
        config.putAll(inConfig.getMap());

        runApplication(config, TransactionalProducerFromIncoming.class);

        List<String> list = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.STRING)
                .consumerName("test-consumer")
                .subscriptionName("test-subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic(topic)
                .subscribe(), 30, msg -> list.add(msg.getValue()));
        await().untilAsserted(() -> assertThat(list).hasSize(30));
    }

    @ApplicationScoped
    public static class TransactionalProducerFromIncoming {

        @Inject
        @Channel("transactional-producer")
        PulsarTransactions<String> transaction;

        @Incoming("in")
        Uni<Void> produceInTransaction(String msg) {
            return transaction.withTransaction(emitter -> {
                emitter.send(PulsarMessage.of(msg, "1"));
                emitter.send(PulsarMessage.of(msg, "2"));
                emitter.send(PulsarMessage.of(msg, "3"));
                return Uni.createFrom().voidItem();
            });
        }

    }

    @Test
    void testMultipleTransactionalConsumer() throws PulsarAdminException, PulsarClientException {
        String inTopic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(inTopic, 3);

        send(client.newProducer(Schema.STRING)
                .producerName("producer")
                .topic(inTopic)
                .create(), 10, (i, producer) -> producer.newMessage().value("v-" + i));

        MapBasedConfig inConfig = baseConfig()
                .with("mp.messaging.incoming.in.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.in.topic", inTopic)
                .with("mp.messaging.incoming.in.subscriptionInitialPosition", "Earliest")
                .with("mp.messaging.incoming.in.schema", "STRING");

        MapBasedConfig outconfig = config();
        MapBasedConfig outconfig2 = configInt();
        MapBasedConfig config = new MapBasedConfig(outconfig.getMap());
        config.putAll(inConfig.getMap());
        config.putAll(outconfig2.getMap());

        runApplication(config, MultipleTransactionalProducersFromIncoming.class);

        List<String> list = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.STRING)
                .consumerName("test-consumer")
                .subscriptionName("test-subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic(topic)
                .subscribe(), 30, msg -> list.add(msg.getValue()));
        await().untilAsserted(() -> assertThat(list).hasSize(30));

        List<Integer> listInt = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .consumerName("test-consume-int")
                .subscriptionName("test-subscription-int")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic(topic + "-int")
                .subscribe(), 30, msg -> listInt.add(msg.getValue()));
        await().untilAsserted(() -> assertThat(listInt).hasSize(30));
    }

    @ApplicationScoped
    public static class MultipleTransactionalProducersFromIncoming {

        @Inject
        @Channel("transactional-producer")
        PulsarTransactions<String> txn1;

        @Inject
        @Channel("transactional-producer-int")
        PulsarTransactions<Integer> txn2;

        @Incoming("in")
        Uni<Void> produceInTransaction(String msg) {
            return txn1.withTransaction(emitter -> {
                emitter.send(PulsarMessage.of(msg, "1"));
                emitter.send(PulsarMessage.of(msg, "2"));
                emitter.send(PulsarMessage.of(msg, "3"));
                txn2.send(emitter, PulsarMessage.of(1, msg));
                txn2.send(emitter, PulsarMessage.of(2, msg));
                txn2.send(emitter, PulsarMessage.of(3, msg));
                return Uni.createFrom().voidItem();
            });
        }

    }
}
