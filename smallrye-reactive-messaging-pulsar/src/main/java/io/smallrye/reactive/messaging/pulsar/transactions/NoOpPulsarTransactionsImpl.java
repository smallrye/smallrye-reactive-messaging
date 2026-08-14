package io.smallrye.reactive.messaging.pulsar.transactions;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.pulsar.client.api.transaction.Transaction;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterImpl;

/**
 * A no-op implementation of {@link PulsarTransactions} used when the channel is not backed by the Pulsar connector
 * (e.g. in testing with an in-memory connector).
 * <p>
 * Transaction semantics are skipped. Messages sent through the {@link TransactionalEmitter}
 * are dispatched via the standard emitter path.
 */
public class NoOpPulsarTransactionsImpl<T> extends MutinyEmitterImpl<T> implements PulsarTransactions<T> {

    private final AtomicInteger activeTransactions = new AtomicInteger(0);

    public NoOpPulsarTransactionsImpl(EmitterConfiguration config, long defaultBufferSize) {
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
                    .eventually(activeTransactions::decrementAndGet);
        } catch (Exception e) {
            activeTransactions.decrementAndGet();
            throw e;
        }
    }

    @Override
    public <R> Uni<R> withTransaction(Duration txnTimeout, Function<TransactionalEmitter<T>, Uni<R>> work) {
        return withTransaction(work);
    }

    @Override
    @CheckReturnValue
    public <R> Uni<R> withTransaction(Message<?> message, Function<TransactionalEmitter<T>, Uni<R>> work) {
        return withTransaction(work);
    }

    @Override
    public <R> Uni<R> withTransaction(Duration txnTimeout, Message<?> message,
            Function<TransactionalEmitter<T>, Uni<R>> work) {
        return withTransaction(work);
    }

    @Override
    public <M extends Message<? extends T>> void send(TransactionalEmitter<?> emitter, M msg) {
        sendMessage(msg).subscribe().with(unused -> {
        }, throwable -> {
        });
    }

    @Override
    public void send(TransactionalEmitter<?> emitter, T payload) {
        send(emitter, Message.of(payload));
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
            CompletableFuture<Void> future = NoOpPulsarTransactionsImpl.this.sendMessage(msg)
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

        @Override
        public Transaction getTransaction(String producerName) {
            return null;
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
