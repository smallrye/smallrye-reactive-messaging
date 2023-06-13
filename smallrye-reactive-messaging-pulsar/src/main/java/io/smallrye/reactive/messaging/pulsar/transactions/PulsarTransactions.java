package io.smallrye.reactive.messaging.pulsar.transactions;

import java.time.Duration;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterType;

@Experimental("Experimental API")
public interface PulsarTransactions<T> extends EmitterType {

    /**
     * Produce records in a Pulsar transaction.
     * <p>
     * The given processing function receives a {@link TransactionalEmitter} for producing records,
     * and returns a {@code Uni} that will provide the result for a successful transaction.
     * <p>
     * If this method is called on a Vert.x context, the processing function is also called on that context.
     * Otherwise, it is called on the sending thread of the producer.
     * <p>
     * If the processing completes successfully, the producer is flushed and the transaction is committed.
     * If the processing throws an exception, returns a failing {@code Uni}, or marks the {@link TransactionalEmitter} for
     * abort,
     * the transaction is aborted.
     *
     * @param work the processing function for producing records.
     * @param <R> the return type
     * @return the {@code Uni} representing the result of the transaction.
     *         If the transaction completes successfully, it will complete with the item returned from the work function.
     *         If the transaction completes with failure, it will fail with the reason.
     * @throws IllegalStateException if a transaction is already in progress.
     */
    @CheckReturnValue
    <R> Uni<R> withTransaction(Function<TransactionalEmitter<T>, Uni<R>> work);

    /**
     * Produce records in a Pulsar transaction.
     * <p>
     * The given processing function receives a {@link TransactionalEmitter} for producing records,
     * and returns a {@code Uni} that will provide the result for a successful transaction.
     * <p>
     * If this method is called on a Vert.x context, the processing function is also called on that context.
     * Otherwise, it is called on the sending thread of the producer.
     * <p>
     * If the processing completes successfully, the producer is flushed and the transaction is committed.
     * If the processing throws an exception, returns a failing {@code Uni}, or marks the {@link TransactionalEmitter} for
     * abort,
     * the transaction is aborted.
     *
     * @param txnTimeout the timeout of the transaction
     * @param work the processing function for producing records.
     * @param <R> the return type
     * @return the {@code Uni} representing the result of the transaction.
     *         If the transaction completes successfully, it will complete with the item returned from the work function.
     *         If the transaction completes with failure, it will fail with the reason.
     */
    <R> Uni<R> withTransaction(Duration txnTimeout, Function<TransactionalEmitter<T>, Uni<R>> work);

    /**
     * Produce records in a Pulsar transaction, by processing the given message exactly-once.
     * <p>
     * If the processing completes successfully, before committing the transaction,
     * the topic partition offsets of the given message will be committed to the transaction.
     * If the processing needs to abort, after aborting the transaction,
     * the consumer's position is reset to the last committed offset, effectively resuming the consumption from that offset.
     * <p>
     *
     * @param message the incoming Pulsar message.
     * @param work the processing function for producing records.
     * @return the {@code Uni} representing the result of the transaction.
     *         If the transaction completes successfully, it will complete with the item returned from the work function.
     *         If the transaction completes with failure, it will fail with the reason.
     * @throws IllegalStateException if a transaction is already in progress.
     */
    @CheckReturnValue
    <R> Uni<R> withTransaction(Message<?> message, Function<TransactionalEmitter<T>, Uni<R>> work);

    /**
     * Produce records in a Pulsar transaction, by processing the given message exactly-once.
     * <p>
     * If the processing completes successfully, before committing the transaction,
     * the topic partition offsets of the given message will be committed to the transaction.
     * If the processing needs to abort, after aborting the transaction,
     * the consumer's position is reset to the last committed offset, effectively resuming the consumption from that offset.
     * <p>
     *
     * @param txnTimeout the timeout of the transaction
     * @param message the incoming Pulsar message.
     * @param work function for producing records.
     * @param <R> type of the return
     * @return the {@code Uni} representing the result of the transaction.
     *         If the transaction completes successfully, it will complete with the item returned from the work function.
     *         If the transaction completes with failure, it will fail with the reason.
     */
    <R> Uni<R> withTransaction(Duration txnTimeout, Message<?> message, Function<TransactionalEmitter<T>, Uni<R>> work);

    /**
     * Produce records in a Pulsar transaction, by processing the given batch message exactly-once.
     * This is a convenience method that ignores the item returned by the transaction and returns the {@code Uni} which
     * <ul>
     * <li>acks the given message if the transaction is successful.</li>
     * <li>nacks the given message if the transaction is aborted.</li>
     * </ul>
     *
     * @param message the incoming Pulsar message.
     * @param work function for producing records.
     * @return the {@code Uni} acking or negative acking the message depending on the result of the transaction.
     * @throws IllegalStateException if a transaction is already in progress.
     * @see #withTransaction(Message, Function)
     */
    @CheckReturnValue
    default Uni<Void> withTransactionAndAck(Message<?> message, Function<TransactionalEmitter<T>, Uni<Void>> work) {
        return withTransaction(message, work)
                .onFailure().recoverWithUni(throwable -> Uni.createFrom().completionStage(message.nack(throwable)));
    }

    /**
     * Produce records in a Pulsar transaction, by processing the given batch message exactly-once.
     * This is a convenience method that ignores the item returned by the transaction and returns the {@code Uni} which
     * <ul>
     * <li>acks the given message if the transaction is successful.</li>
     * <li>nacks the given message if the transaction is aborted.</li>
     * </ul>
     *
     * @param txnTimeout the timeout of the transaction
     * @param message the incoming Pulsar message.
     * @param work function for producing records.
     * @return the {@code Uni} acking or negative acking the message depending on the result of the transaction.
     */
    @CheckReturnValue
    default Uni<Void> withTransactionAndAck(Duration txnTimeout, Message<?> message,
            Function<TransactionalEmitter<T>, Uni<Void>> work) {
        return withTransaction(txnTimeout, message, work)
                .onFailure().recoverWithUni(throwable -> Uni.createFrom().completionStage(message.nack(throwable)));
    }

    /**
     * Send message to an already started transaction
     *
     * @param emitter the emitter which holds the transaction
     * @param msg the message to send
     * @param <M> the type of the message to send
     */
    <M extends Message<? extends T>> void send(TransactionalEmitter<?> emitter, M msg);

    /**
     * Send message to an already started transaction
     *
     * @param emitter the emitter which holds the transaction
     * @param payload the payload to send
     */
    void send(TransactionalEmitter<?> emitter, T payload);

    /**
     * @return {@code true} if a transaction is in progress.
     */
    boolean isTransactionInProgress();

}
