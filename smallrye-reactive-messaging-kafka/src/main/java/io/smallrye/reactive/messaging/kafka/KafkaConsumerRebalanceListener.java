package io.smallrye.reactive.messaging.kafka;

import java.time.Duration;
import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;

/**
 * When implemented by a managed bean annotated with {@link io.smallrye.common.annotation.Identifier @Identifier}
 * and configured against an inbound connector, it will be applied as a consumer rebalance listener
 * to that inbound connector's consumer.
 * <p>
 * To configure which listener you want to use, set the name in the inbound connector's consumer rebalance listener
 * attribute. For example:
 *
 * <pre>
 * mp.messaging.incoming.example.consumer-rebalance-listener.name=ExampleConsumerRebalanceListener
 *
 * &#64;Identifier("ExampleConsumerRebalanceListener")
 * public class ExampleConsumerRebalanceListener implements KafkaConsumerRebalanceListener {
 *     ...
 * }
 * </pre>
 *
 * Alternatively, name your listener (using the {@code @Identifier} annotation) to be the group id used by the connector.
 * For example:
 *
 * <pre>
 * mp.messaging.incoming.example.group.id=my-group
 *
 * &#64;Identifier("my-group")
 * public class MyGroupConsumerRebalanceListener implements KafkaConsumerRebalanceListener {
 *     ...
 * }
 * </pre>
 * <p>
 * Setting the consumer rebalance listener name takes precedence over using the group id.
 *
 * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener
 */
public interface KafkaConsumerRebalanceListener {

    /**
     * A callback method the user can implement to provide handling of customized offsets on completion of a successful
     * partition re-assignment. This method will be called after the partition re-assignment completes and before the
     * consumer starts fetching data, and only as the result of a {@link Consumer#poll(java.time.Duration) poll(long)} call.
     * <p>
     * It is guaranteed that under normal conditions all the processes in a consumer group will execute their
     * {@link #onPartitionsRevoked(Consumer, Collection)} callback before any instance executes this
     * callback. During exceptional scenarios, partitions may be migrated
     * without the old owner being notified (i.e. their {@link #onPartitionsRevoked(Consumer, Collection)} callback not
     * triggered),
     * and later when the old owner consumer realized this event, the {@link #onPartitionsLost(Consumer, Collection)}
     * (Collection)} callback
     * will be triggered by the consumer then.
     * <p>
     * It is common for the assignment callback to use the consumer instance in order to query offsets. It is possible
     * for a {@link org.apache.kafka.common.errors.WakeupException} or {@link org.apache.kafka.common.errors.InterruptException}
     * to be raised from one of these nested invocations. In this case, the exception will be propagated to the current
     * invocation of {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(java.time.Duration)} in which this callback is
     * being executed. This means it is not
     * necessary to catch these exceptions and re-attempt to wakeup or interrupt the consumer thread.
     *
     * <strong>IMPORTANT</strong>: The behavior must be blocking. Callback invoked from the polling thread.
     *
     * @param partitions The list of partitions that are now assigned to the consumer (previously owned partitions will
     *        NOT be included, i.e. this list will only include newly added partitions)
     * @throws org.apache.kafka.common.errors.WakeupException If raised from a nested call to
     *         {@link org.apache.kafka.clients.consumer.KafkaConsumer}
     * @throws org.apache.kafka.common.errors.InterruptException If raised from a nested call to
     *         {@link org.apache.kafka.clients.consumer.KafkaConsumer}
     */
    default void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<org.apache.kafka.common.TopicPartition> partitions) {

    }

    /**
     * A callback method the user can implement to provide handling of offset commits to a customized store.
     * This method will be called during a rebalance operation when the consumer has to give up some partitions.
     * It can also be called when consumer is being closed
     * ({@link org.apache.kafka.clients.consumer.KafkaConsumer#close(Duration)})
     * or is unsubscribing ({@link org.apache.kafka.clients.consumer.KafkaConsumer#unsubscribe()}).
     * It is recommended that offsets should be committed in this callback to either Kafka or a
     * custom offset store to prevent duplicate data.
     * <p>
     * In eager rebalancing, it will always be called at the start of a rebalance and after the consumer stops fetching data.
     * In cooperative rebalancing, it will be called at the end of a rebalance on the set of partitions being revoked iff the
     * set is non-empty.
     * For examples on usage of this API, see Usage Examples section of {@link org.apache.kafka.clients.consumer.KafkaConsumer
     * KafkaConsumer}.
     * <p>
     * It is common for the revocation callback to use the consumer instance in order to commit offsets. It is possible
     * for a {@link org.apache.kafka.common.errors.WakeupException} or {@link org.apache.kafka.common.errors.InterruptException}
     * to be raised from one of these nested invocations. In this case, the exception will be propagated to the current
     * invocation of {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(java.time.Duration)} in which this callback is
     * being executed. This means it is not
     * necessary to catch these exceptions and re-attempt to wakeup or interrupt the consumer thread.
     *
     * <strong>IMPORTANT</strong>: The behavior must be blocking. Callback invoked from the polling thread.
     *
     * @param partitions The list of partitions that were assigned to the consumer and now need to be revoked (may not
     *        include all currently assigned partitions, i.e. there may still be some partitions left)
     * @throws org.apache.kafka.common.errors.WakeupException If raised from a nested call to
     *         {@link org.apache.kafka.clients.consumer.KafkaConsumer}
     * @throws org.apache.kafka.common.errors.InterruptException If raised from a nested call to
     *         {@link org.apache.kafka.clients.consumer.KafkaConsumer}
     */
    default void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<org.apache.kafka.common.TopicPartition> partitions) {

    }

    /**
     * A callback method you can implement to provide handling of cleaning up resources for partitions that have already
     * been reassigned to other consumers. This method will not be called during normal execution as the owned partitions would
     * first be revoked by calling the {@link #onPartitionsRevoked}, before being reassigned
     * to other consumers during a rebalance event. However, during exceptional scenarios when the consumer realized that it
     * does not own this partition any longer, i.e. not revoked via a normal rebalance event, then this method would be invoked.
     * <p>
     * For example, this function is called if a consumer's session timeout has expired, or if a fatal error has been
     * received indicating the consumer is no longer part of the group.
     * <p>
     * By default it will just trigger {@link #onPartitionsRevoked}; for users who want to distinguish
     * the handling logic of revoked partitions v.s. lost partitions, they can override the default implementation.
     * <p>
     * It is possible
     * for a {@link org.apache.kafka.common.errors.WakeupException} or {@link org.apache.kafka.common.errors.InterruptException}
     * to be raised from one of these nested invocations. In this case, the exception will be propagated to the current
     * invocation of {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(java.time.Duration)} in which this callback is
     * being executed. This means it is not
     * necessary to catch these exceptions and re-attempt to wakeup or interrupt the consumer thread.
     *
     * @param partitions The list of partitions that were assigned to the consumer and now have been reassigned
     *        to other consumers. With the current protocol this will always include all of the consumer's
     *        previously assigned partitions, but this may change in future protocols (ie there would still
     *        be some partitions left)
     * @throws org.apache.kafka.common.errors.WakeupException If raised from a nested call to
     *         {@link org.apache.kafka.clients.consumer.KafkaConsumer}
     * @throws org.apache.kafka.common.errors.InterruptException If raised from a nested call to
     *         {@link org.apache.kafka.clients.consumer.KafkaConsumer}
     */
    default void onPartitionsLost(Consumer<?, ?> consumer, Collection<org.apache.kafka.common.TopicPartition> partitions) {
        onPartitionsRevoked(consumer, partitions);
    }

}
