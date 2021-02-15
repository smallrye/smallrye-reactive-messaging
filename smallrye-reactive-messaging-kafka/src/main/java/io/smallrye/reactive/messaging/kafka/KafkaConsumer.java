package io.smallrye.reactive.messaging.kafka;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.Uni;

/**
 * Kafka Consumer API.
 * <p>
 * Unlike {@link Consumer}, this API is asynchronous and make sure that the actions are executed on the <em>Kafka</em> polling
 * thread.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
public interface KafkaConsumer<K, V> {

    /**
     * Runs an action on the polling thread.
     * <p>
     * The action is a function taking as parameter the {@link Consumer} and that returns a result (potentially {@code null}).
     * The produced {@link Uni} emits the returned result when the action completes. If the action throws an exception,
     * the produced using emits the exception as failure.
     * <p>
     * If the action does not return a result, use {@link #runOnPollingThread(Consumer)}.
     *
     * @param action the action to execute, must not be {@code null}
     * @param <R> the type of result, can be {@code Void}
     * @return the Uni emitting the result or the failure when the action completes.
     */
    <R> Uni<R> runOnPollingThread(Function<Consumer<K, V>, R> action);

    /**
     * Runs an action on the polling thread.
     * <p>
     * The action is a consumer receiving the {@link Consumer}.
     * The produced {@link Uni} emits {@code null} when the action completes. If the action throws an exception,
     * the produced using emits the exception as failure.
     *
     * @param action the action, must not be {@code null}
     * @return the Uni emitting {@code null} or the failure when the action completes.
     */
    Uni<Void> runOnPollingThread(java.util.function.Consumer<Consumer<K, V>> action);

    /**
     * Pauses the consumption of records.
     * The polling will continue, but no records will be received.
     *
     * @return the Uni emitting when the action completes, the set of topic/partition paused by this call.
     */
    Uni<Set<TopicPartition>> pause();

    /**
     * Retrieves the set of paused topic/partition
     *
     * @return the Uni emitting the set of topic/partition paused.
     */
    Uni<Set<TopicPartition>> paused();

    /**
     * Retrieved the last committed offset for each topic/partition
     *
     * @param tps the set of topic/partition to query, must not be {@code null}, must not be empty.
     * @return the Uni emitting the offset for the underlying consumer for each of the passed topic/partition.
     */
    Uni<Map<TopicPartition, OffsetAndMetadata>> committed(TopicPartition... tps);

    /**
     * Resumes the consumption of record.
     * It resumes the consumption of all the paused topic/partition.
     *
     * @return the Uni indicating when the resume action completes.
     */
    Uni<Void> resume();

    /**
     * @return the underlying consumer. Be aware that to use it you needs to be on the polling thread.
     */
    Consumer<K, V> unwrap();

    /**
     * Commits the offsets
     *
     * @param map the map of topic/partition -> offset to commit
     * @return the Uni emitting {@code null} when the commit has been executed.
     */
    Uni<Void> commit(
            Map<TopicPartition, OffsetAndMetadata> map);

    /**
     * Retrieves the next positions for each assigned topic/partitions
     *
     * @return the Uni emitting the map of topic/partition -> position.
     */
    Uni<Map<TopicPartition, Long>> getPositions();

    /**
     * Retrieves the current assignments of the consumer.
     *
     * @return the Uni emitting the set of topic/partition currently assigned to the consumer
     */
    Uni<Set<TopicPartition>> getAssignments();
}
