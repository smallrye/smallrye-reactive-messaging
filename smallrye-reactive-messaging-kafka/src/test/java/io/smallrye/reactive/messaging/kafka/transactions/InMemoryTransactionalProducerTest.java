package io.smallrye.reactive.messaging.kafka.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.common.errors.TransactionAbortedException;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.base.WeldTestBase;
import io.smallrye.reactive.messaging.memory.InMemoryConnector;
import io.smallrye.reactive.messaging.memory.InMemorySink;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class InMemoryTransactionalProducerTest extends WeldTestBase {

    @AfterEach
    void cleanup() {
        InMemoryConnector.clear();
    }

    @Test
    void testKafkaTransactionsWithInMemoryConnector() {
        addBeans(InMemoryConnector.class);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.tx-out.connector", InMemoryConnector.CONNECTOR);

        TransactionalProducerApp application = runApplication(config, TransactionalProducerApp.class);

        application.produceInTransaction(5).await().indefinitely();

        InMemoryConnector connector = getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        InMemorySink<Integer> sink = connector.sink("tx-out");

        List<? extends Message<Integer>> received = sink.received();
        assertThat(received).hasSize(5);
        assertThat(received).extracting(Message::getPayload).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void testKafkaTransactionsWithInMemoryConnectorBlocking() {
        addBeans(InMemoryConnector.class);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.tx-out.connector", InMemoryConnector.CONNECTOR);

        TransactionalProducerApp application = runApplication(config, TransactionalProducerApp.class);

        application.produceInTransactionBlocking(5);

        InMemoryConnector connector = getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        InMemorySink<Integer> sink = connector.sink("tx-out");

        List<? extends Message<Integer>> received = sink.received();
        assertThat(received).hasSize(5);
        assertThat(received).extracting(Message::getPayload).containsExactly(0, 1, 2, 3, 4);
    }

    @ApplicationScoped
    public static class TransactionalProducerApp {

        @Inject
        @Channel("tx-out")
        KafkaTransactions<Integer> transaction;

        Uni<Void> produceInTransaction(int count) {
            return transaction.withTransaction(emitter -> {
                for (int i = 0; i < count; i++) {
                    emitter.send(i);
                }
                return Uni.createFrom().voidItem();
            });
        }

        void produceInTransactionBlocking(int count) {
            transaction.withTransactionAndAwait(emitter -> {
                for (int i = 0; i < count; i++) {
                    emitter.send(i);
                }
                return Uni.createFrom().voidItem();
            });
        }

        public KafkaTransactions<Integer> transaction() {
            return transaction;
        }
    }

    @Test
    void testKafkaTransactionsAbort() {
        addBeans(InMemoryConnector.class);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.tx-out.connector", InMemoryConnector.CONNECTOR);

        TransactionalProducerApp application = runApplication(config, TransactionalProducerApp.class);

        assertThatThrownBy(() -> application.transaction().withTransaction(emitter -> {
            emitter.send(1);
            emitter.markForAbort();
            return Uni.createFrom().voidItem();
        }).await().indefinitely())
                .isInstanceOf(TransactionAbortedException.class);

        assertThat(application.transaction().isTransactionInProgress()).isFalse();
    }

    @Test
    void testKafkaTransactionsIsNoOpImpl() {
        addBeans(InMemoryConnector.class);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.tx-out.connector", InMemoryConnector.CONNECTOR);

        TransactionalProducerApp application = runApplication(config, TransactionalProducerApp.class);

        assertThat(application.transaction()).isInstanceOf(NoOpKafkaTransactionsImpl.class);
        assertThat(application.transaction().isTransactionInProgress()).isFalse();

        application.transaction()
                .withTransaction((Message<?>) null, emitter -> {
                    emitter.send(42);
                    return Uni.createFrom().voidItem();
                })
                .await().indefinitely();

        InMemoryConnector connector = getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        assertThat(connector.sink("tx-out").received()).hasSize(1);
    }
}
