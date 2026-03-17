package io.smallrye.reactive.messaging.kafka.transactions;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TransactionAbortedException;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.KafkaProducer;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordBatchMetadata;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging;
import io.smallrye.reactive.messaging.kafka.impl.TopicPartitions;
import io.smallrye.reactive.messaging.kafka.impl.TransactionScopeMetadata;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterImpl;
import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

public class KafkaTransactionsImpl<T> extends MutinyEmitterImpl<T> implements KafkaTransactions<T> {

    private static final Duration DEFAULT_TRANSACTION_TIMEOUT = Duration.ofSeconds(60);

    private final KafkaClientService clientService;
    private final KafkaProducer<?, ?> producer;
    private final Duration transactionTimeout;

    private final AtomicInteger activeTransactions = new AtomicInteger(0);

    public KafkaTransactionsImpl(EmitterConfiguration config, long defaultBufferSize, KafkaClientService clientService) {
        super(config, defaultBufferSize);
        this.clientService = clientService;
        this.producer = clientService.getProducer(config.name());
        this.transactionTimeout = resolveTransactionTimeout(producer.configuration());
    }

    private static Duration resolveTransactionTimeout(Map<String, ?> config) {
        Object value = config.get(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
        if (value != null) {
            return Duration.ofMillis(Long.parseLong(value.toString()));
        }
        return DEFAULT_TRANSACTION_TIMEOUT;
    }

    @Override
    public boolean isTransactionInProgress() {
        return activeTransactions.get() > 0;
    }

    @Override
    @CheckReturnValue
    public <R> Uni<R> withTransaction(Function<TransactionalEmitter<T>, Uni<R>> work) {
        return runAsync(createTransaction(), work);
    }

    @Override
    public <R> R withTransactionAndAwait(Function<TransactionalEmitter<T>, Uni<R>> work) {
        return runBlocking(createTransaction(), work);
    }

    @SuppressWarnings("rawtypes")
    @Override
    @CheckReturnValue
    public <R> Uni<R> withTransaction(Message<?> message, Function<TransactionalEmitter<T>, Uni<R>> work) {
        Optional<IncomingKafkaRecordBatchMetadata> batchMetadata = message
                .getMetadata(IncomingKafkaRecordBatchMetadata.class);
        Optional<IncomingKafkaRecordMetadata> recordMetadata = message.getMetadata(IncomingKafkaRecordMetadata.class);
        if (batchMetadata.isPresent()) {
            return withTransaction(batchMetadata.get(), work);
        } else if (recordMetadata.isPresent()) {
            return withTransaction(recordMetadata.get(), work);
        } else {
            throw KafkaExceptions.ex.noKafkaMetadataFound(message);
        }
    }

    @Override
    @CheckReturnValue
    public <R> Uni<R> withTransaction(IncomingKafkaRecordMetadata<?, ?> metadata,
            Function<TransactionalEmitter<T>, Uni<R>> work) {
        return runAsync(createExactlyOnceTransaction(metadata), work);
    }

    @Override
    public <R> R withTransactionAndAwait(IncomingKafkaRecordMetadata<?, ?> metadata,
            Function<TransactionalEmitter<T>, Uni<R>> work) {
        return runBlocking(createExactlyOnceTransaction(metadata), work);
    }

    private <R> Transaction<R> createExactlyOnceTransaction(IncomingKafkaRecordMetadata<?, ?> metadata) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TopicPartitions.getTopicPartition(metadata.getTopic(), metadata.getPartition()),
                new OffsetAndMetadata(metadata.getOffset() + 1));
        return createExactlyOnceTransaction(metadata.getChannel(), offsets,
                metadata.getConsumerGroupGenerationId(), metadata.getPartition());
    }

    @Override
    @CheckReturnValue
    public <R> Uni<R> withTransaction(IncomingKafkaRecordBatchMetadata<?, ?> metadata,
            Function<TransactionalEmitter<T>, Uni<R>> work) {
        return runAsync(createExactlyOnceTransaction(metadata), work);
    }

    @Override
    public <R> R withTransactionAndAwait(IncomingKafkaRecordBatchMetadata<?, ?> metadata,
            Function<TransactionalEmitter<T>, Uni<R>> work) {
        return runBlocking(createExactlyOnceTransaction(metadata), work);
    }

    private <R> Transaction<R> createExactlyOnceTransaction(IncomingKafkaRecordBatchMetadata<?, ?> metadata) {
        Map<TopicPartition, OffsetAndMetadata> offsets = metadata.getOffsets().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue().offset() + 1)));
        return createExactlyOnceTransaction(metadata.getChannel(), offsets, metadata.getConsumerGroupGenerationId(), -1);
    }

    private <R> Transaction<R> createTransaction() {
        if (producer.isPooled()) {
            KafkaProducer<?, ?> scope = producer.transactionScope();
            return new Transaction<>(scope, new TransactionScopeMetadata(scope));
        } else {
            return new Transaction<>(producer);
        }
    }

    private KafkaConsumer<Object, Object> resolveConsumer(String channel) {
        List<KafkaConsumer<Object, Object>> consumers = clientService.getConsumers(channel);
        if (consumers.isEmpty()) {
            throw KafkaExceptions.ex.unableToFindConsumerForChannel(channel);
        } else if (consumers.size() > 1) {
            throw KafkaExceptions.ex.exactlyOnceProcessingNotSupported(channel);
        }
        return consumers.get(0);
    }

    private <R> Transaction<R> createExactlyOnceTransaction(
            String channel,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            int generationId,
            int defaultPartition) {
        final KafkaProducer<?, ?> txProd;
        final TransactionScopeMetadata scopeMetadata;
        final int partition;
        final Function<Throwable, Uni<R>> afterAbort;

        KafkaConsumer<Object, Object> consumer = resolveConsumer(channel);
        if (producer.isPooled()) {
            KafkaProducer<?, ?> scope = producer.transactionScope();
            txProd = scope;
            scopeMetadata = new TransactionScopeMetadata(scope);
            partition = defaultPartition;
            // Only reset the specific partition(s) involved,
            // not all assigned partitions, to avoid interfering with
            // concurrent pooled transactions on other partitions
            afterAbort = t -> consumer.resetToLastCommittedPositions(offsets.keySet())
                    .chain(() -> Uni.createFrom().failure(t));
        } else {
            txProd = producer;
            scopeMetadata = null;
            partition = -1;
            afterAbort = t -> consumer.resetToLastCommittedPositions()
                    .chain(() -> Uni.createFrom().failure(t));
        }

        Uni<Void> beforeCommit = consumer.consumerGroupMetadata().chain(groupMetadata -> {
            if (groupMetadata.generationId() == generationId) {
                return txProd.sendOffsetsToTransaction(offsets, groupMetadata);
            } else {
                return Uni.createFrom().failure(
                        KafkaExceptions.ex.exactlyOnceProcessingRebalance(channel, groupMetadata.toString(),
                                String.valueOf(generationId)));
            }
        });

        return new Transaction<>(txProd, scopeMetadata, partition,
                beforeCommit, r -> Uni.createFrom().item(r),
                VOID_UNI, afterAbort);
    }

    /**
     * Run a transaction asynchronously, returning a {@code Uni} that completes when the transaction finishes.
     * The work function is dispatched via {@link ContextExecutor} to preserve the caller's Vert.x context.
     */
    private <R> Uni<R> runAsync(Transaction<R> tx, Function<TransactionalEmitter<T>, Uni<R>> work) {
        if (producer.isPooled()) {
            activeTransactions.incrementAndGet();
        } else if (!activeTransactions.compareAndSet(0, 1)) {
            throw KafkaExceptions.ex.transactionInProgress(name);
        }
        try {
            return tx.execute(work)
                    .eventually(activeTransactions::decrementAndGet);
        } catch (Exception e) {
            activeTransactions.decrementAndGet();
            throw e;
        }
    }

    /**
     * Run a transaction imperatively on the caller thread.
     * Kafka operations block while executing on the producer's internal sending thread.
     */
    private <R> R runBlocking(Transaction<R> tx, Function<TransactionalEmitter<T>, Uni<R>> work) {
        if (producer.isPooled()) {
            activeTransactions.incrementAndGet();
        } else if (!activeTransactions.compareAndSet(0, 1)) {
            throw KafkaExceptions.ex.transactionInProgress(name);
        }
        try {
            return tx.executeBlocking(work);
        } finally {
            activeTransactions.decrementAndGet();
        }
    }

    private static final Uni<Void> VOID_UNI = Uni.createFrom().voidItem();

    private static <R> Uni<R> defaultAfterCommit(R result) {
        return Uni.createFrom().item(result);
    }

    private static <R> Uni<R> defaultAfterAbort(Throwable throwable) {
        return Uni.createFrom().failure(throwable);
    }

    private class Transaction<R> implements TransactionalEmitter<T> {

        private final KafkaProducer<?, ?> txProducer;
        private final Uni<Void> beforeCommit;
        private final Function<R, Uni<R>> afterCommit;

        private final Uni<Void> beforeAbort;
        private final Function<Throwable, Uni<R>> afterAbort;

        // Non-null when this transaction uses a pooled transaction scope.
        // Attached as metadata to outgoing messages so KafkaSink routes sends through the scope.
        private final TransactionScopeMetadata scopeMetadata;

        // Default partition derived from the incoming record, applied to outgoing records
        // that don't have an explicit partition set. -1 means no default.
        private final int defaultPartition;

        private final List<Uni<Void>> sendUnis = new CopyOnWriteArrayList<>();
        private volatile boolean abort;

        public Transaction(KafkaProducer<?, ?> txProducer) {
            this(txProducer, null, -1, VOID_UNI, KafkaTransactionsImpl::defaultAfterCommit, VOID_UNI,
                    KafkaTransactionsImpl::defaultAfterAbort);
        }

        public Transaction(KafkaProducer<?, ?> txProducer, TransactionScopeMetadata scope) {
            this(txProducer, scope, -1, VOID_UNI, KafkaTransactionsImpl::defaultAfterCommit, VOID_UNI,
                    KafkaTransactionsImpl::defaultAfterAbort);
        }

        public Transaction(KafkaProducer<?, ?> txProducer, TransactionScopeMetadata scopeMetadata, int defaultPartition,
                Uni<Void> beforeCommit, Function<R, Uni<R>> afterCommit,
                Uni<Void> beforeAbort, Function<Throwable, Uni<R>> afterAbort) {
            this.txProducer = txProducer;
            this.scopeMetadata = scopeMetadata;
            this.defaultPartition = defaultPartition;
            this.beforeCommit = beforeCommit;
            this.afterCommit = afterCommit;
            this.beforeAbort = beforeAbort;
            this.afterAbort = afterAbort;
        }

        Uni<R> execute(Function<TransactionalEmitter<T>, Uni<R>> work) {
            final ContextExecutor executor = new ContextExecutor();
            return txProducer.beginTransaction()
                    .plug(executor::emitOn)
                    .chain(() -> completeTransaction(Uni.createFrom().nullItem().chain(() -> work.apply(this))))
                    .plug(executor::emitOn);
        }

        /**
         * Execute the transaction imperatively.
         * {@code beginTransaction} and the work function run on the caller thread;
         * the remaining steps (waitOnSend, flush, commit/abort, callbacks) are
         * chained reactively via {@link #completeTransaction} and awaited once,
         * bounded by {@code transaction.timeout.ms}.
         */
        R executeBlocking(Function<TransactionalEmitter<T>, Uni<R>> work) {
            txProducer.beginTransaction().await().indefinitely();
            Uni<R> workResult;
            try {
                R result = work.apply(this).await().indefinitely();
                workResult = Uni.createFrom().item(result);
            } catch (Exception e) {
                workResult = Uni.createFrom().failure(e);
            }
            return completeTransaction(workResult).await().atMost(transactionTimeout);
        }

        /**
         * Shared transaction completion chain used by both the async and blocking paths.
         * Waits for sends, flushes, commits or aborts, and invokes the after-callbacks.
         */
        private Uni<R> completeTransaction(Uni<R> workResult) {
            //noinspection Convert2MethodRef
            return workResult
                    // wait until all send operations are completed
                    .eventually(() -> waitOnSend())
                    // only flush() if the work completed with no exception
                    .call(() -> txProducer.flush())
                    // in the case of an exception or cancellation
                    // we need to rollback the transaction
                    .onFailure().call(throwable -> abort())
                    .onCancellation().call(() -> abort())
                    // when there was no exception,
                    // commit or rollback the transaction
                    .call(() -> abort ? abort() : commit().onFailure().recoverWithUni(throwable -> {
                        KafkaLogging.log.transactionCommitFailed(throwable);
                        return abort();
                    }))
                    // finally, call after commit or after abort callbacks
                    .onFailure().recoverWithUni(throwable -> afterAbort.apply(throwable))
                    .onItem().transformToUni(result -> afterCommit.apply(result));
        }

        private Uni<List<Void>> waitOnSend() {
            return sendUnis.isEmpty() ? Uni.createFrom().nullItem() : Uni.join().all(sendUnis).andCollectFailures();
        }

        private Uni<Void> commit() {
            return beforeCommit.call(txProducer::commitTransaction);
        }

        private Uni<Void> abort() {
            Uni<Void> uni = beforeAbort.call(txProducer::abortTransaction);
            return abort ? uni.chain(() -> Uni.createFrom().failure(new TransactionAbortedException())) : uni;
        }

        @Override
        public <M extends Message<? extends T>> void send(M msg) {
            Message<? extends T> messageToSend = msg;
            if (defaultPartition >= 0) {
                messageToSend = addDefaultPartition(messageToSend);
            }
            if (scopeMetadata != null) {
                messageToSend = messageToSend.addMetadata(scopeMetadata);
            }
            CompletableFuture<Void> send = KafkaTransactionsImpl.this.sendMessage(messageToSend)
                    .onFailure().invoke(KafkaLogging.log::unableToSendRecord)
                    .subscribeAsCompletionStage();
            sendUnis.add(Uni.createFrom().completionStage(send));
        }

        @Override
        public void send(T payload) {
            send(Message.of(payload));
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private <M extends Message<?>> M addDefaultPartition(M msg) {
            Optional<OutgoingKafkaRecordMetadata> existing = msg.getMetadata(OutgoingKafkaRecordMetadata.class);
            if (existing.isPresent() && existing.get().getPartition() > -1) {
                return msg; // explicit partition already set
            }
            OutgoingKafkaRecordMetadata<?> partitionMeta;
            if (existing.isPresent()) {
                // Preserve existing fields (key, topic, timestamp, headers), add partition
                partitionMeta = OutgoingKafkaRecordMetadata.from(existing.get())
                        .withPartition(defaultPartition)
                        .build();
            } else {
                partitionMeta = OutgoingKafkaRecordMetadata.builder()
                        .withPartition(defaultPartition)
                        .build();
            }
            return (M) msg.addMetadata(partitionMeta);
        }

        @Override
        public void markForAbort() {
            abort = true;
        }

        @Override
        public boolean isMarkedForAbort() {
            return abort;
        }
    }

    /**
     * An executor that captures the caller Vert.x context and whether the caller is on an event loop thread or worker thread.
     * Runs the command on the Vert.x event loop thread if the current thread is an event loop thread.
     * <p>
     * And if run on worker thread, `work` is called on the worker thread.
     */
    private static class ContextExecutor implements Executor {
        private final Context context;
        private final boolean ioThread;

        ContextExecutor() {
            this(Vertx.currentContext(), Context.isOnEventLoopThread());
        }

        ContextExecutor(Context context, boolean ioThread) {
            this.context = context;
            this.ioThread = ioThread;
        }

        <T> Uni<T> emitOn(Uni<T> uni) {
            return context == null ? uni : uni.emitOn(this);
        }

        @Override
        public void execute(Runnable command) {
            if (context == null) {
                command.run();
            } else {
                if (ioThread) {
                    VertxContext.runOnContext(context, command);
                } else {
                    VertxContext.executeBlocking(context, command);
                }
            }
        }
    }

}
