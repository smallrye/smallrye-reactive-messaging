package io.smallrye.reactive.messaging;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;

/**
 * A binding between a channel publisher and a typed payload consumer.
 * <p>
 * Created via {@link ChannelFactory#bind(java.util.concurrent.Flow.Publisher, Class)},
 * this provides a fluent API to subscribe to a channel with automatic payload conversion
 * and ack/nack handling.
 * <p>
 * On each message:
 * <ul>
 * <li>The payload is converted to the target type using available {@link MessageConverter}s</li>
 * <li>The consumer is invoked with the converted payload (and optionally the message {@link Metadata})</li>
 * <li>On success, the message is acknowledged</li>
 * <li>On failure, the message is negatively acknowledged with the exception</li>
 * </ul>
 *
 * @param <I> the payload type
 * @param <O> the output type, used when a processor is set via {@link #process(Function)} or {@link #process(BiFunction)}
 */
public interface ChannelBinding<I, O> {

    /**
     * Terminal operation that acknowledges each processed message and discards it.
     * If a processor was set via {@link #process(Function)}, it is applied before acknowledging.
     * On failure, the message is negatively acknowledged.
     *
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable subscribe();

    /**
     * Convenience for {@code .process(consumer).subscribe()}.
     * Sets the consumer as a processor and subscribes, acknowledging each message after
     * the consumer completes successfully.
     *
     * @param consumer the payload consumer
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable subscribe(Consumer<O> consumer);

    /**
     * Convenience for {@code .process(consumer).subscribe()}.
     * Sets the consumer as a processor and subscribes, acknowledging each message after
     * the consumer completes successfully.
     *
     * @param consumer the payload and metadata consumer
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable subscribe(BiConsumer<O, Metadata> consumer);

    /**
     * Convenience for {@code .process(consumer).subscribe()}.
     * Sets the consumer as a processor and subscribes, acknowledging each message after
     * the returned {@link Uni} completes.
     *
     * @param consumer the async payload consumer returning a {@link Uni}
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable subscribe(Function<O, Uni<Void>> consumer);

    /**
     * Convenience for {@code .process(consumer).subscribe()}.
     * Sets the consumer as a processor and subscribes, acknowledging each message after
     * the returned {@link Uni} completes.
     *
     * @param consumer the async payload and metadata consumer returning a {@link Uni}
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable subscribe(BiFunction<O, Metadata, Uni<Void>> consumer);

    /**
     * Returns a new {@link ChannelBinding} that dispatches the consumer to a worker thread pool.
     * Messages are processed sequentially.
     * Use this when the consumer performs blocking operations.
     *
     * @return a new blocking {@link ChannelBinding}
     */
    ChannelBinding<I, O> blocking();

    /**
     * Returns a new {@link ChannelBinding} that dispatches the consumer to a worker thread pool,
     * processing up to {@code concurrency} messages in parallel.
     * Use this when the consumer performs blocking operations.
     *
     * @param concurrency the maximum number of messages processed concurrently
     * @return a new blocking {@link ChannelBinding}
     */
    ChannelBinding<I, O> blocking(int concurrency);

    /**
     * Returns a new {@link ChannelBinding} that dispatches the consumer to the given executor.
     * Messages are processed sequentially.
     * Use this when the consumer performs blocking operations.
     *
     * @param executor the executor to run the consumer on
     * @return a new blocking {@link ChannelBinding}
     */
    ChannelBinding<I, O> blocking(Executor executor);

    /**
     * Returns a new {@link ChannelBinding} that dispatches the consumer to the given executor,
     * processing up to {@code concurrency} messages in parallel.
     * Use this when the consumer performs blocking operations.
     *
     * @param executor the executor to run the consumer on
     * @param concurrency the maximum number of messages processed concurrently
     * @return a new blocking {@link ChannelBinding}
     */
    ChannelBinding<I, O> blocking(Executor executor, int concurrency);

    /**
     * Sets a transformation function on this binding.
     * The processor transforms each incoming payload before forwarding.
     * Use {@link #to(Flow.Subscriber)} to wire the processed stream to an outgoing channel.
     *
     * @param processor the payload transformation function
     * @return this {@link ChannelBinding} with the processor set
     */
    <T> ChannelBinding<I, T> process(Function<I, T> processor);

    /**
     * Sets a transformation function with metadata access on this binding.
     * Use {@link #to(Flow.Subscriber)} to wire the processed stream to an outgoing channel.
     *
     * @param processor the payload and metadata transformation function
     * @return this {@link ChannelBinding} with the processor set
     */
    <T> ChannelBinding<I, T> process(BiFunction<I, Metadata, T> processor);

    /**
     * Sets an async transformation function on this binding.
     * Use {@link #to(Flow.Subscriber)} to wire the processed stream to an outgoing channel.
     *
     * @param processor the async payload transformation function
     * @return this {@link ChannelBinding} with the processor set
     */
    <T> ChannelBinding<I, T> processAsync(Function<I, Uni<T>> processor);

    /**
     * Sets an async transformation function with metadata access on this binding.
     * Use {@link #to(Flow.Subscriber)} to wire the processed stream to an outgoing channel.
     *
     * @param processor the async payload and metadata transformation function
     * @return this {@link ChannelBinding} with the processor set
     */
    <T> ChannelBinding<I, T> processAsync(BiFunction<I, Metadata, Uni<T>> processor);

    /**
     * Wires the stream to an outgoing subscriber.
     * If {@link #process(Function)} was called, each message is transformed before forwarding.
     * Otherwise, messages are forwarded as-is.
     * The outgoing message preserves the incoming message's metadata and ack/nack chain.
     *
     * @param subscriber the outgoing channel subscriber,
     *        typically obtained from {@link ChannelFactory#outgoing(String, org.eclipse.microprofile.config.Config)}
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable to(Flow.Subscriber<? extends Message<?>> subscriber);

    /**
     * Creates an outbound channel from the given configuration and wires this binding to it.
     * Combines {@link ChannelFactory#outgoing(String, Config)} and {@link #to(Flow.Subscriber)} in one call.
     *
     * @param channel the outgoing channel name
     * @param config the connector configuration (must contain a {@code connector} attribute)
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable to(String channel, Config config);

    /**
     * Creates an outbound channel from the given channel-specific configuration map,
     * and wires this binding to it.
     * Connector-wide defaults from the application configuration are used as fallback.
     *
     * @param channel the outgoing channel name
     * @param channelConfig flat map of channel-specific configuration properties
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable to(String channel, Map<String, String> channelConfig);
}
