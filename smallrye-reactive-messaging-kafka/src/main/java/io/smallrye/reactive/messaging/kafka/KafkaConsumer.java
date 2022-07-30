package io.smallrye.reactive.messaging.kafka;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.CheckReturnValue;
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
     * @return Kafka consumer configuration
     */
    Map<String, ?> configuration();

    /**
     * Runs an action on the polling thread.
     * <p>
     * The action is a function taking as parameter the {@link Consumer} and that returns a result (potentially {@code null}).
     * The produced {@link Uni} emits the returned result when the action completes. If the action throws an exception,
     * the produced {@code Uni} emits the exception as failure.
     * <p>
     * If the action does not return a result, use {@link #runOnPollingThread(java.util.function.Consumer)}.
     *
     * @param action the action to execute, must not be {@code null}
     * @param <R> the type of result, can be {@code Void}
     * @return the Uni emitting the result or the failure when the action completes.
     */
    @CheckReturnValue
    <R> Uni<R> runOnPollingThread(Function<Consumer<K, V>, R> action);

    /**
     * Runs an action on the polling thread.
     * <p>
     * The action is a consumer receiving the {@link Consumer}.
     * The produced {@link Uni} emits {@code null} when the action completes. If the action throws an exception,
     * the produced {@code Uni} emits the exception as failure.
     *
     * @param action the action, must not be {@code null}
     * @return the Uni emitting {@code null} or the failure when the action completes.
     */
    @CheckReturnValue
    Uni<Void> runOnPollingThread(java.util.function.Consumer<Consumer<K, V>> action);

    /**
     * Pauses the consumption of records.
     * The polling will continue, but no records will be received.
     *
     * <strong>IMPORTANT:</strong> To use this method, you need to disable the auto-pause/resume feature from the connector.
     * Otherwise, the client will be resumed automatically when there are enough requests. To disable the auto-pause/resume,
     * set {@code mp.messaging.incoming.[channel].pause-if-no-requests} to {@code false}.
     *
     * @return the Uni emitting when the action completes, the set of topic/partition paused by this call.
     */
    @CheckReturnValue
    Uni<Set<TopicPartition>> pause();

    /**
     * Retrieves the set of paused topic/partition
     *
     * @return the Uni emitting the set of topic/partition paused.
     */
    @CheckReturnValue
    Uni<Set<TopicPartition>> paused();

    /**
     * Retrieved the last committed offset for each topic/partition
     *
     * @param tps the set of topic/partition to query, must not be {@code null}, must not be empty.
     * @return the Uni emitting the offset for the underlying consumer for each of the passed topic/partition.
     */
    @CheckReturnValue
    Uni<Map<TopicPartition, OffsetAndMetadata>> committed(TopicPartition... tps);

    /**
     * Resumes the consumption of record.
     * It resumes the consumption of all the paused topic/partition.
     *
     * <strong>IMPORTANT:</strong> To use this method, you need to disable the auto-pause/resume feature from the connector.
     * Otherwise, the client will be paused automatically when there are no requests. To disable the auto-pause/resume,
     * set {@code mp.messaging.incoming.[channel].pause-if-no-requests} to {@code false}.
     *
     * @return the Uni indicating when the resume action completes.
     */
    @CheckReturnValue
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
    @CheckReturnValue
    Uni<Void> commit(
            Map<TopicPartition, OffsetAndMetadata> map);

    /**
     * Commits the offsets asynchronously
     *
     * @param map the map of topic/partition -> offset to commit
     * @return the Uni emitting {@code null} when the commit has been executed.
     */
    Uni<Void> commitAsync(Map<TopicPartition, OffsetAndMetadata> map);

    /**
     * Retrieves the next positions for each assigned topic/partitions
     *
     * @return the Uni emitting the map of topic/partition -> position.
     */
    @CheckReturnValue
    Uni<Map<TopicPartition, Long>> getPositions();

    /**
     * Retrieves the current assignments of the consumer.
     *
     * @return the Uni emitting the set of topic/partition currently assigned to the consumer
     */
    @CheckReturnValue
    Uni<Set<TopicPartition>> getAssignments();

    /**
     * Overrides the fetch offset that the consumer will use on the next poll of given topic and partition.
     * Note that you may lose data if this API is arbitrarily used in the middle of consumption.
     *
     * @param partition the topic and partition for which to set the offset
     * @param offset the new offset
     * @return a Uni that completes successfully when the offset is set;
     *         it completes with {@code IllegalArgumentException} if the provided offset is negative;
     *         it completes with {@code IllegalStateException} if the provided {@code TopicPartition} is not assigned to this
     *         consumer
     */
    @CheckReturnValue
    Uni<Void> seek(TopicPartition partition, long offset);

    /**
     * Overrides the fetch offset that the consumer will use on the next poll of given topic and partition.
     * This method allows setting the leaderEpoch along with the desired offset.
     * Note that you may lose data if this API is arbitrarily used in the middle of consumption.
     *
     * @param partition the topic and partition for which to set the offset
     * @param offsetAndMetadata the new offset, with additional metadata
     * @return a Uni that completes successfully when the offset is set;
     *         it completes with {@code IllegalArgumentException} if the provided offset is negative;
     *         it completes with {@code IllegalStateException} if the provided {@code TopicPartition} is not assigned to this
     *         consumer
     */
    @CheckReturnValue
    Uni<Void> seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);

    /**
     * Seek to the first offset for each of the given partitions.
     * If no partitions are provided, seek to the first offset for all of the currently assigned partitions.
     *
     * @param partitions the partitions for which to set the offset
     * @return a Uni that completes successfully when the offset is set;
     *         it completes with {@code IllegalArgumentException} if {@code partitions} is {@code null};
     *         it completes with {@code IllegalStateException} if any of the provided {@code TopicPartition}s are not currently
     *         assigned to this consumer
     */
    @CheckReturnValue
    Uni<Void> seekToBeginning(Collection<TopicPartition> partitions);

    /**
     * Seek to the last offset for each of the given partitions.
     * If no partitions are provided, seek to the last offset for all of the currently assigned partitions.
     *
     * @param partitions the partitions for which to set the offset
     * @return a Uni that completes successfully when the offset is set;
     *         it completes with {@code IllegalArgumentException} if {@code partitions} is {@code null};
     *         it completes with {@code IllegalStateException} if any of the provided {@code TopicPartition}s are not currently
     *         assigned to this consumer
     */
    @CheckReturnValue
    Uni<Void> seekToEnd(Collection<TopicPartition> partitions);

    /**
     * @return the Uni emitting the group metadata of this consumer
     */
    @CheckReturnValue
    Uni<ConsumerGroupMetadata> consumerGroupMetadata();

    /**
     * For each assigned topic partition reset to last committed offset or the beginning
     *
     * @return the Uni emitting {@code null} when the reset has been executed.
     */
    @CheckReturnValue
    Uni<Void> resetToLastCommittedPositions();

}
