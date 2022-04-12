package io.smallrye.reactive.messaging.kafka.transactions;

import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterType;

/**
 * Emitter interface for producing records in a Kafka transaction.
 * This interface defines a custom Emitter for implementing Kafka Producer Transactions and Exactly-once processing.
 * <p>
 * Transactional producer:
 *
 * <pre>
 * <code>
 *
 * &#64;Inject @Channel("tx-out-channel") KafkaTransactions&lt;String&gt; txEmitter;
 *
 * // ...
 * Uni&lt;Integer&gt; txWork = txEmitter.withTransaction(emitter -> {
 *     emitter.send("a");
 *     emitter.send("b");
 *     emitter.send("c");
 *     return Uni.createFrom().item(3);
 * });
 *
 * // ...
 * </code>
 * </pre>
 *
 * <p>
 * Exactly-once processing:
 *
 * <pre>
 * <code>
 *
 * &#64;Inject @Channel("tx-out-channel") KafkaTransactions&lt;String&gt; txEmitter;
 *
 * &#64;Incoming("in-channel")
 * Uni&lt;Void&gt; process(KafkaRecordBatch&lt;String, String&gt; batch) {
 *     return txEmitter.withTransaction(batch, emitter -> {
 *         for (KafkaRecord&lt;String, String&gt; rec : batch) {
 *           txEmitter.send(KafkaRecord.of(record.getKey(), record.getPayload()));
 *         }
 *         return Uni.createFrom().voidItem();
 *     });
 * }
 * </code>
 * </pre>
 *
 * @param <T> emitted payload type
 */
@Experimental("Experimental API")
public interface KafkaTransactions<T> extends EmitterType {

    /**
     * Produce records in a Kafka transaction.
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
     * Produce records in a Kafka transaction, by processing the given message exactly-once.
     * <p>
     * If the processing completes successfully, before committing the transaction,
     * the topic partition offsets of the given message will be committed to the transaction.
     * If the processing needs to abort, after aborting the transaction,
     * the consumer's position is reset to the last committed offset, effectively resuming the consumption from that offset.
     * <p>
     *
     * @param message the incoming Kafka message expected to contain a metadata {@code IncomingKafkaRecordBatchMetadata} or
     *        {@code IncomingKafkaRecordMetadata}.
     * @param work the processing function for producing records.
     * @return the {@code Uni} representing the result of the transaction.
     *         If the transaction completes successfully, it will complete with the item returned from the work function.
     *         If the transaction completes with failure, it will fail with the reason.
     * @throws IllegalStateException if a transaction is already in progress.
     */
    @CheckReturnValue
    <R> Uni<R> withTransaction(Message<?> message, Function<TransactionalEmitter<T>, Uni<R>> work);

    /**
     * Produce records in a Kafka transaction, by processing the given batch message exactly-once.
     * This is a convenience method that ignores the item returned by the transaction and returns the {@code Uni} which
     * <ul>
     * <li>acks the given message if the transaction is successful.</li>
     * <li>nacks the given message if the transaction is aborted.</li>
     * </ul>
     *
     * @param batchMessage the batch message expected to contain a metadata of {@code IncomingKafkaRecordBatchMetadata}.
     * @param work function for producing records.
     * @return the {@code Uni} acking or negative acking the message depending on the result of the transaction.
     * @throws IllegalStateException if a transaction is already in progress.
     * @see #withTransaction(Message, Function)
     */
    @CheckReturnValue
    default Uni<Void> withTransactionAndAck(Message<?> batchMessage, Function<TransactionalEmitter<T>, Uni<Void>> work) {
        return withTransaction(batchMessage, work)
                .onFailure().recoverWithUni(throwable -> Uni.createFrom().completionStage(batchMessage.nack(throwable)))
                .onItem().transformToUni(unused -> Uni.createFrom().completionStage(batchMessage.ack()));
    }

    /**
     * @return {@code true} if a transaction is in progress.
     */
    boolean isTransactionInProgress();

}
