package io.smallrye.reactive.messaging.kafka;

import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.*;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;

/**
 * Kafka Share Consumer API.
 * <p>
 * Unlike {@link ShareConsumer}, this API is asynchronous and makes sure that the actions are executed on the
 * <em>Kafka</em> polling thread.
 * <p>
 * Share consumers are designed for cooperative consumption across multiple consumers without explicit partition assignment.
 * Records are automatically distributed among share group members, and acknowledgment is managed through commits.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
public interface KafkaShareConsumer<K, V> {

    /**
     * @return Kafka share consumer configuration
     */
    Map<String, ?> configuration();

    /**
     * Runs an action on the polling thread.
     * <p>
     * The action is a function taking as parameter the {@link KafkaShareConsumer} and that returns a result (potentially
     * {@code null}).
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
    <R> Uni<R> runOnPollingThread(Function<ShareConsumer<K, V>, R> action);

    /**
     * Runs an action on the polling thread.
     * <p>
     * The action is a consumer receiving the {@link KafkaShareConsumer}.
     * The produced {@link Uni} emits {@code null} when the action completes. If the action throws an exception,
     * the produced {@code Uni} emits the exception as failure.
     *
     * @param action the action, must not be {@code null}
     * @return the Uni emitting {@code null} or the failure when the action completes.
     */
    @CheckReturnValue
    Uni<Void> runOnPollingThread(java.util.function.Consumer<ShareConsumer<K, V>> action);

    /**
     * @return the underlying share consumer. Be aware that to use it you needs to be on the polling thread.
     */
    ShareConsumer<K, V> unwrap();

    /**
     * Commits the offsets
     *
     * @return the Uni emitting {@code null} when the commit has been executed.
     */
    @CheckReturnValue
    Uni<Void> commit();

    /**
     * Commits the offsets asynchronously
     *
     * @return the Uni emitting {@code null} when the commit has been executed.
     */
    Uni<Void> commitAsync();

    /**
     *
     * @return
     */
    Uni<Void> acknowledge(ConsumerRecord<K, V> record);

    Uni<Void> acknowledge(ConsumerRecord<K, V> record, AcknowledgeType type);

}
