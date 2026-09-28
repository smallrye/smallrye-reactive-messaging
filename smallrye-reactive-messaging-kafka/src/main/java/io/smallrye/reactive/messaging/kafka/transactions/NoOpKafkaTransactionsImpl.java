package io.smallrye.reactive.messaging.kafka.transactions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.kafka.common.errors.TransactionAbortedException;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordBatchMetadata;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterImpl;

/**
 * A no-op implementation of {@link KafkaTransactions} used when the channel is not backed by the Kafka connector
 * (e.g. in testing with an in-memory connector).
 * <p>
 * Transaction semantics (begin, commit, abort) are skipped. Messages sent through the
 * {@link TransactionalEmitter} are dispatched via the standard emitter path, reaching whatever
 * downstream subscriber is wired to the channel.
 */
public class NoOpKafkaTransactionsImpl<T> extends MutinyEmitterImpl<T> implements KafkaTransactions<T> {

    private final AtomicInteger activeTransactions = new AtomicInteger(0);

    public NoOpKafkaTransactionsImpl(EmitterConfiguration config, long defaultBufferSize) {
        super(config, defaultBufferSize);
    }

    @Override
    @CheckReturnValue
    public <R> Uni<R> withTransaction(Function<TransactionalEmitter<T>, Uni<R>> work) {
        if (!activeTransactions.compareAndSet(0, 1)) {
            throw new IllegalStateException("A transaction is already in progress for channel '" + name + "'");
        }
        try {
            NoOpTransactionalEmitter emitter = new NoOpTransactionalEmitter();
            return work.apply(emitter)
                    .call(() -> emitter.waitOnSends())
                    .chain(result -> {
                        if (emitter.isMarkedForAbort()) {
                            return Uni.createFrom().failure(new TransactionAbortedException());
                        }
                        return Uni.createFrom().item(result);
                    })
                    .eventually(activeTransactions::decrementAndGet);
        } catch (Exception e) {
            activeTransactions.decrementAndGet();
            throw e;
        }
    }

    @Override
    public <R> R withTransactionAndAwait(Function<TransactionalEmitter<T>, Uni<R>> work) {
        return withTransaction(work).await().indefinitely();
    }

    @Override
    @CheckReturnValue
    public <R> Uni<R> withTransaction(Message<?> message, Function<TransactionalEmitter<T>, Uni<R>> work) {
        return withTransaction(work);
    }

    @Override
    @CheckReturnValue
    public <R> Uni<R> withTransaction(IncomingKafkaRecordMetadata<?, ?> metadata,
            Function<TransactionalEmitter<T>, Uni<R>> work) {
        return withTransaction(work);
    }

    @Override
    public <R> R withTransactionAndAwait(IncomingKafkaRecordMetadata<?, ?> metadata,
            Function<TransactionalEmitter<T>, Uni<R>> work) {
        return withTransactionAndAwait(work);
    }

    @Override
    @CheckReturnValue
    public <R> Uni<R> withTransaction(IncomingKafkaRecordBatchMetadata<?, ?> metadata,
            Function<TransactionalEmitter<T>, Uni<R>> work) {
        return withTransaction(work);
    }

    @Override
    public <R> R withTransactionAndAwait(IncomingKafkaRecordBatchMetadata<?, ?> metadata,
            Function<TransactionalEmitter<T>, Uni<R>> work) {
        return withTransactionAndAwait(work);
    }

    @Override
    public boolean isTransactionInProgress() {
        return activeTransactions.get() > 0;
    }

    private class NoOpTransactionalEmitter implements TransactionalEmitter<T> {

        private final CopyOnWriteArrayList<Uni<Void>> sendUnis = new CopyOnWriteArrayList<>();
        private volatile boolean abort;

        @Override
        public <M extends Message<? extends T>> void send(M msg) {
            CompletableFuture<Void> future = NoOpKafkaTransactionsImpl.this.sendMessage(msg)
                    .subscribeAsCompletionStage();
            sendUnis.add(Uni.createFrom().completionStage(future));
        }

        @Override
        public void send(T payload) {
            send(Message.of(payload));
        }

        @Override
        public void markForAbort() {
            abort = true;
        }

        @Override
        public boolean isMarkedForAbort() {
            return abort;
        }

        Uni<Void> waitOnSends() {
            if (sendUnis.isEmpty()) {
                return Uni.createFrom().voidItem();
            }
            return Uni.join().all(sendUnis).andCollectFailures()
                    .replaceWithVoid();
        }
    }
}
