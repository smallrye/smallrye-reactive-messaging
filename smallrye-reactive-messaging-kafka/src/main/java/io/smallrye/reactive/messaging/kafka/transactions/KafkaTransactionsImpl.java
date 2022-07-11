package io.smallrye.reactive.messaging.kafka.transactions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
import io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterImpl;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

public class KafkaTransactionsImpl<T> extends MutinyEmitterImpl<T> implements KafkaTransactions<T> {

    private final KafkaClientService clientService;
    private final KafkaProducer<?, ?> producer;

    private volatile Transaction<?> currentTransaction;

    public KafkaTransactionsImpl(EmitterConfiguration config, long defaultBufferSize, KafkaClientService clientService) {
        super(config, defaultBufferSize);
        this.clientService = clientService;
        this.producer = clientService.getProducer(config.name());
    }

    @Override
    public synchronized boolean isTransactionInProgress() {
        return currentTransaction != null;
    }

    @Override
    @CheckReturnValue
    public synchronized <R> Uni<R> withTransaction(Function<TransactionalEmitter<T>, Uni<R>> work) {
        if (currentTransaction == null) {
            return new Transaction<R>().execute(work);
        }
        throw KafkaExceptions.ex.transactionInProgress(name);
    }

    @SuppressWarnings("rawtypes")
    @Override
    @CheckReturnValue
    public synchronized <R> Uni<R> withTransaction(Message<?> message, Function<TransactionalEmitter<T>, Uni<R>> work) {
        String channel;
        int consumerIndex;
        Map<TopicPartition, OffsetAndMetadata> offsets;

        Optional<IncomingKafkaRecordBatchMetadata> batchMetadata = message.getMetadata(IncomingKafkaRecordBatchMetadata.class);
        Optional<IncomingKafkaRecordMetadata> recordMetadata = message.getMetadata(IncomingKafkaRecordMetadata.class);
        if (batchMetadata.isPresent()) {
            IncomingKafkaRecordBatchMetadata<?, ?> metadata = batchMetadata.get();
            channel = metadata.getChannel();
            consumerIndex = metadata.getConsumerIndex();
            offsets = metadata.getOffsets().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue().offset() + 1)));
        } else if (recordMetadata.isPresent()) {
            IncomingKafkaRecordMetadata<?, ?> metadata = recordMetadata.get();
            channel = metadata.getChannel();
            consumerIndex = metadata.getConsumerIndex();
            offsets = new HashMap<>();
            offsets.put(new TopicPartition(metadata.getTopic(), metadata.getPartition()),
                    new OffsetAndMetadata(metadata.getOffset() + 1));
        } else {
            throw KafkaExceptions.ex.noKafkaMetadataFound(message);
        }

        List<KafkaConsumer<Object, Object>> consumers = clientService.getConsumers(channel);
        if (consumers.isEmpty() || (consumerIndex != -1 && consumerIndex >= consumers.size())) {
            throw KafkaExceptions.ex.unableToFindConsumerForChannel(channel);
        }
        KafkaConsumer<Object, Object> consumer = consumers.get(consumerIndex == -1 ? 0 : consumerIndex);
        if (currentTransaction == null) {
            return new Transaction<R>(
                    /* before commit */
                    consumer.consumerGroupMetadata()
                            .chain(groupMetadata -> producer.sendOffsetsToTransaction(offsets, groupMetadata)),
                    r -> Uni.createFrom().item(r),
                    VOID_UNI,
                    /* after abort */
                    t -> consumer.resetToLastCommittedPositions()
                            .chain(() -> Uni.createFrom().failure(t))).execute(work);
        }
        throw KafkaExceptions.ex.transactionInProgress(name);
    }

    private static final Uni<Void> VOID_UNI = Uni.createFrom().voidItem();

    private static <R> Uni<R> defaultAfterCommit(R result) {
        return Uni.createFrom().item(result);
    }

    private static <R> Uni<R> defaultAfterAbort(Throwable throwable) {
        return Uni.createFrom().failure(throwable);
    }

    private class Transaction<R> implements TransactionalEmitter<T> {

        private final Uni<Void> beforeCommit;
        private final Function<R, Uni<R>> afterCommit;

        private final Uni<Void> beforeAbort;
        private final Function<Throwable, Uni<R>> afterAbort;

        private volatile boolean abort;

        public Transaction() {
            this(VOID_UNI, KafkaTransactionsImpl::defaultAfterCommit, VOID_UNI, KafkaTransactionsImpl::defaultAfterAbort);
        }

        public Transaction(Uni<Void> beforeCommit, Function<R, Uni<R>> afterCommit,
                Uni<Void> beforeAbort, Function<Throwable, Uni<R>> afterAbort) {
            this.beforeCommit = beforeCommit;
            this.afterCommit = afterCommit;
            this.beforeAbort = beforeAbort;
            this.afterAbort = afterAbort;
        }

        Uni<R> execute(Function<TransactionalEmitter<T>, Uni<R>> work) {
            currentTransaction = this;
            // If run on Vert.x context, `work` is called on the same context.
            Context context = Vertx.currentContext();
            Uni<Void> beginTx = producer.beginTransaction();
            if (context != null) {
                beginTx = beginTx.emitOn(runnable -> context.runOnContext(x -> runnable.run()));
            }
            return beginTx
                    .chain(() -> executeInTransaction(work))
                    .eventually(() -> currentTransaction = null);
        }

        private Uni<R> executeInTransaction(Function<TransactionalEmitter<T>, Uni<R>> work) {
            //noinspection Convert2MethodRef
            return Uni.createFrom().nullItem()
                    .chain(() -> work.apply(this))
                    // only flush() if the work completed with no exception
                    .call(() -> producer.flush())
                    // in the case of an exception or cancellation
                    // we need to rollback the transaction
                    .onFailure().call(throwable -> abort())
                    .onCancellation().call(() -> abort())
                    // when there was no exception,
                    // commit or rollback the transaction
                    .call(() -> abort ? abort() : commit())
                    // finally, call after commit or after abort callbacks
                    .onFailure().recoverWithUni(throwable -> afterAbort.apply(throwable))
                    .onItem().transformToUni(result -> afterCommit.apply(result));
        }

        private Uni<Void> commit() {
            return beforeCommit.call(producer::commitTransaction);
        }

        private Uni<Void> abort() {
            Uni<Void> uni = beforeAbort.call(producer::abortTransaction);
            return abort ? uni.chain(() -> Uni.createFrom().failure(new TransactionAbortedException())) : uni;
        }

        @Override
        public <M extends Message<? extends T>> void send(M msg) {
            KafkaTransactionsImpl.this.send(msg.withNack(throwable -> CompletableFuture.completedFuture(null)));
        }

        @Override
        public void send(T payload) {
            KafkaTransactionsImpl.this.send(payload).subscribe().with(unused -> {
            }, KafkaLogging.log::unableToSendRecord);
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

}
