package io.smallrye.reactive.messaging.pulsar.transactions;

import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionBuilder;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterImpl;
import io.smallrye.reactive.messaging.pulsar.PulsarClientService;
import io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

public class PulsarTransactionsImpl<T> extends MutinyEmitterImpl<T> implements PulsarTransactions<T> {

    private final PulsarClient pulsarClient;

    private final AtomicInteger txnCount = new AtomicInteger();
    private final PulsarClientService pulsarClientService;

    public PulsarTransactionsImpl(EmitterConfiguration config, long defaultBufferSize,
            PulsarClientService pulsarClientService) {
        super(config, defaultBufferSize);
        this.pulsarClientService = pulsarClientService;
        this.pulsarClient = pulsarClientService.getClient(config.name());
    }

    @Override
    public <R> Uni<R> withTransaction(Function<TransactionalEmitter<T>, Uni<R>> work) {
        return new PulsarTransactionEmitter<R>().execute(work);
    }

    @Override
    public <R> Uni<R> withTransaction(Duration txnTimeout, Function<TransactionalEmitter<T>, Uni<R>> work) {
        return new PulsarTransactionEmitter<R>(txnTimeout).execute(work);
    }

    @Override
    public <R> Uni<R> withTransaction(Message<?> message, Function<TransactionalEmitter<T>, Uni<R>> work) {
        return withTransaction(null, message, work);
    }

    @Override
    public <R> Uni<R> withTransaction(Duration txnTimeout, Message<?> message, Function<TransactionalEmitter<T>, Uni<R>> work) {
        return new PulsarTransactionEmitter<R>(txnTimeout,
                txn -> Uni.createFrom().completionStage(message.ack()),
                PulsarTransactionsImpl::defaultAfterCommit,
                (txn, throwable) -> {
                    if (!(throwable.getCause() instanceof PulsarClientException.TransactionConflictException)) {
                        // If TransactionConflictException is not thrown,
                        // you need to redeliver or negativeAcknowledge this message,
                        // or else this message will not be received again.
                        return Uni.createFrom().completionStage(() -> message.nack(throwable));
                    } else {
                        return VOID_UNI;
                    }
                },
                PulsarTransactionsImpl::defaultAfterAbort)
                .execute(e -> {
                    if (message instanceof MetadataInjectableMessage) {
                        ((MetadataInjectableMessage<?>) message)
                                .injectMetadata(new PulsarTransactionMetadata(e.getTransaction(null)));
                    }
                    return work.apply(e);
                });
    }

    @Override
    public <M extends Message<? extends T>> void send(TransactionalEmitter<?> emitter, M msg) {
        Transaction transaction = emitter.getTransaction(this.name);
        PulsarTransactionsImpl.this.sendMessage(msg.addMetadata(new PulsarTransactionMetadata(transaction)))
                .subscribe().with(unused -> {
                }, PulsarLogging.log::unableToDispatch);
    }

    @Override
    public void send(TransactionalEmitter<?> emitter, T payload) {
        this.send(emitter, Message.of(payload));
    }

    @Override
    public boolean isTransactionInProgress() {
        return txnCount.get() != 0;
    }

    private static final Uni<Void> VOID_UNI = Uni.createFrom().voidItem();

    private static <R> Uni<R> defaultAfterCommit(R result) {
        return Uni.createFrom().item(result);
    }

    private static <R> Uni<R> defaultAfterAbort(Throwable throwable) {
        return Uni.createFrom().failure(throwable);
    }

    private class PulsarTransactionEmitter<R> implements TransactionalEmitter<T> {

        private final Function<Transaction, Uni<Void>> beforeCommit;
        private final Function<R, Uni<R>> afterCommit;
        private final BiFunction<Transaction, Throwable, Uni<Void>> beforeAbort;
        private final Function<Throwable, Uni<R>> afterAbort;

        private final Duration txnTimeout;

        private volatile boolean abort;

        private volatile Transaction currentTransaction;

        private final Set<String> producerChannels = new HashSet<>();

        public PulsarTransactionEmitter() {
            this(null, txn -> VOID_UNI, PulsarTransactionsImpl::defaultAfterCommit,
                    (txn, throwable) -> VOID_UNI,
                    PulsarTransactionsImpl::defaultAfterAbort);
        }

        public PulsarTransactionEmitter(Duration txnTimeout) {
            this(txnTimeout, txn -> VOID_UNI, PulsarTransactionsImpl::defaultAfterCommit,
                    (txn, throwable) -> VOID_UNI,
                    PulsarTransactionsImpl::defaultAfterAbort);
        }

        public PulsarTransactionEmitter(Duration txnTimeout, Function<Transaction, Uni<Void>> beforeCommit,
                Function<R, Uni<R>> afterCommit,
                BiFunction<Transaction, Throwable, Uni<Void>> beforeAbort,
                Function<Throwable, Uni<R>> afterAbort) {
            this.txnTimeout = txnTimeout;
            this.beforeCommit = beforeCommit;
            this.afterCommit = afterCommit;
            this.beforeAbort = beforeAbort;
            this.afterAbort = afterAbort;
        }

        private Uni<Transaction> createTransaction() {
            return Uni.createFrom().emitter(emitter -> {
                TransactionBuilder builder = pulsarClient.newTransaction();
                if (txnTimeout != null) {
                    builder = builder.withTransactionTimeout(txnTimeout.toMillis(), TimeUnit.MILLISECONDS);
                }
                builder.build().whenComplete((transaction, throwable) -> {
                    if (throwable == null) {
                        emitter.complete(transaction);
                    } else {
                        emitter.fail(throwable);
                    }
                });
            });
        }

        private Uni<Void> flushProducers() {
            return Multi.createFrom().iterable(producerChannels)
                    .map(pulsarClientService::getProducer)
                    .filter(Objects::nonNull)
                    .onItem().transformToUniAndMerge(p -> Uni.createFrom().completionStage(p.flushAsync()))
                    .toUni();
        }

        Uni<R> execute(Function<TransactionalEmitter<T>, Uni<R>> work) {
            // If run on Vert.x context, `work` is called on the same context.
            Context context = Vertx.currentContext();
            Uni<Transaction> transaction = createTransaction();
            if (context != null) {
                transaction = transaction.emitOn(runnable -> context.runOnContext(x -> runnable.run()));
            }
            return transaction
                    .invoke(txn -> {
                        currentTransaction = txn;
                        txnCount.incrementAndGet();
                    })
                    .chain(txn -> executeInTransaction(work))
                    .eventually(() -> {
                        txnCount.decrementAndGet();
                        currentTransaction = null;
                    });
        }

        private Uni<R> executeInTransaction(Function<TransactionalEmitter<T>, Uni<R>> work) {
            //noinspection Convert2MethodRef
            return Uni.createFrom().nullItem()
                    .chain(() -> work.apply(this))
                    // only flush() if the work completed with no exception
                    .call(() -> flushProducers())
                    // in the case of an exception or cancellation
                    // we need to rollback the transaction
                    .onFailure().call(throwable -> abort(throwable))
                    .onCancellation().call(() -> abort(new RuntimeException("Transaction cancelled")))
                    // when there was no exception,
                    // commit or rollback the transaction
                    .call(() -> abort ? abort(new RuntimeException("Transaction aborted")) : commit())
                    .onFailure().recoverWithUni(throwable -> afterAbort.apply(throwable))
                    .onItem().transformToUni(result -> afterCommit.apply(result));
        }

        private Uni<Void> commit() {
            return beforeCommit.apply(currentTransaction)
                    .chain(txn -> Uni.createFrom().completionStage(currentTransaction::commit));
        }

        private Uni<Void> abort(Throwable throwable) {
            return beforeAbort.apply(currentTransaction, throwable)
                    .chain(x -> Uni.createFrom().completionStage(currentTransaction::abort))
                    .plug(uni -> abort ? uni.chain(() -> Uni.createFrom().failure(throwable)) : uni);
        }

        @Override
        public <M extends Message<? extends T>> void send(M msg) {
            PulsarTransactionsImpl.this.send(this, msg);
        }

        @Override
        public void send(T payload) {
            this.send(Message.of(payload));
        }

        @Override
        public void markForAbort() {
            abort = true;
        }

        @Override
        public boolean isMarkedForAbort() {
            return abort;
        }

        @Override
        public Transaction getTransaction(String name) {
            if (name != null) {
                producerChannels.add(name);
            }
            return currentTransaction;
        }
    }
}
