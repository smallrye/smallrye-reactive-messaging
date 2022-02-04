package io.smallrye.reactive.messaging.kafka;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiEmitter;

/**
 * Kafka Producer API.
 * <p>
 * Unlike {@link org.apache.kafka.clients.producer.KafkaProducer}, this API is guaranteed to be asynchronous.
 * Note that even though the {@code org.apache.kafka.clients.producer.KafkaProducer} is documented to be asynchronous,
 * it actually may block in some cases; see <a href="https://issues.apache.org/jira/browse/KAFKA-3539">KAFKA-3539</a>
 * for more info.
 * </p>
 * <p>
 * The way asynchrony is guaranteed here is an implementation detail. Currently, the sending actions are executed
 * on a special <em>sending</em> thread, but when KAFKA-3539 is fixed, the implementation may become just a simple
 * wrapper providing a {@code Uni} API.
 * </p>
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
public interface KafkaProducer<K, V> {
    /**
     * Runs an action on the sending thread.
     * <p>
     * The action is a function taking as parameter the {@link Producer} and that returns a result (potentially {@code null}).
     * The produced {@link Uni} emits the returned result when the action completes. If the action throws an exception,
     * the produced {@code Uni} emits the exception as failure.
     * <p>
     * If the action does not return a result, use {@link #runOnSendingThread(java.util.function.Consumer)}.
     *
     * @param action the action to execute, must not be {@code null}
     * @param <R> the type of result, can be {@code Void}
     * @return the Uni emitting the result or the failure when the action completes.
     */
    @CheckReturnValue
    <R> Uni<R> runOnSendingThread(Function<Producer<K, V>, R> action);

    /**
     * Runs an action on the sending thread.
     * <p>
     * The action is a consumer receiving the {@link Producer}.
     * The produced {@link Uni} emits {@code null} when the action completes. If the action throws an exception,
     * the produced {@code Uni} emits the exception as failure.
     *
     * @param action the action, must not be {@code null}
     * @return the Uni emitting {@code null} or the failure when the action completes.
     */
    @CheckReturnValue
    Uni<Void> runOnSendingThread(java.util.function.Consumer<Producer<K, V>> action);

    /**
     * Send a record to a topic. The returned {@link Uni} completes with {@link RecordMetadata} when the send
     * has been acknowledged, or with an exception in case of an error.
     */
    @CheckReturnValue
    Uni<RecordMetadata> send(ProducerRecord<K, V> record);

    /**
     * Sends all buffered records immediately. The returned {@link Uni} completes when all requests belonging
     * to the buffered records complete. In other words, when the returned {@code Uni} completes, all
     * previous {@link #send(ProducerRecord)} operations are known to be complete as well.
     * No guarantee is made about the completion of records sent after {@code flush} was called.
     */
    @CheckReturnValue
    Uni<Void> flush();

    /**
     * Returns a list of partition metadata for given topic.
     */
    @CheckReturnValue
    Uni<List<PartitionInfo>> partitionsFor(String topic);

    /**
     * @return the Uni emitting {@code null} when the init transactions executes.
     */
    Uni<Void> initTransactions();

    /**
     * @return the Uni emitting {@code null} when the begin transactions executes.
     */
    Uni<Void> beginTransaction();

    /**
     * @return the Uni emitting {@code null} when the commit transactions executes.
     */
    Uni<Void> commitTransaction();

    /**
     * @return the Uni emitting {@code null} when the abort transactions executes.
     */
    Uni<Void> abortTransaction();

    /**
     *
     * @param offsets topic partition offsets to commit into transaction
     * @param groupMetadata consumer group metadata of the exactly-once consumer
     * @return the Uni emitting {@code null} when the init transactions executes.
     */
    Uni<Void> sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
            ConsumerGroupMetadata groupMetadata);

    /**
     * Produce records emitted using the {@link MultiEmitter} inside a Kafka producer transaction.
     * <p>
     * The given consumer will be called on the sending thread, after successfully beginning the transaction.
     * If the consumer method returns successfully, the given {@code MultiEmitter} is completed, and the transaction is
     * committed.
     * If the consumer method throws an exception or given {@code MultiEmitter} emits a failure, the transaction is aborted.
     * <p>
     * Note that emitted {@link ProducerRecord}s are lazily sent, meaning that returned {@code Uni} will need to be subscribed
     * for the
     * whole transaction to execute.
     *
     * @param work {@code MultiEmitter} consumer for emitting {@code ProducerRecord}s
     * @return {@code Uni} representing the transaction execution.
     */
    Uni<Void> withTransaction(Consumer<MultiEmitter<? super ProducerRecord<K, V>>> work);

    /**
     * @return the underlying producer. Be aware that to use it you needs to be on the sending thread.
     */
    Producer<K, V> unwrap();
}
