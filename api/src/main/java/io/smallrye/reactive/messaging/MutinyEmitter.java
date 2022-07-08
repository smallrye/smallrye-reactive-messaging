package io.smallrye.reactive.messaging;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;

/**
 * Interface used to feed a channel from an <em>imperative</em> piece of code.
 * <p>
 * Instances are injected using:
 *
 * <pre>
 * &#64;Inject
 * &#64;Channel("my-channel")
 * MutinyEmitter&lt;String&gt; emitter;
 * </pre>
 * <p>
 * You can use an injected emitter to send either payloads or
 * {@link org.eclipse.microprofile.reactive.messaging.Message Messages}.
 * <p>
 * The name of the channel (given in the {@link Channel Channel annotation})
 * indicates which channel is fed. It must match the name used in a method using
 * {@link org.eclipse.microprofile.reactive.messaging.Incoming @Incoming} or an
 * outgoing channel configured in the application configuration.
 * <p>
 * The {@link OnOverflow OnOverflow annotation} can be used to configure what to do if
 * messages are sent using the `MutinyEmitter` when a downstream subscriber hasn't requested
 * more messages.
 *
 * @param <T> type of payload
 */
public interface MutinyEmitter<T> extends EmitterType {

    /**
     * Sends a payload to the channel.
     * <p>
     * A {@link Message} object will be created to hold the payload and the returned {@code Uni}
     * can be subscribed to for triggering the send.
     * When subscribed, a {@code null} item will be passed to the {@code Uni} when the
     * {@code Message} is acknowledged. If the {@code Message} is never acknowledged, then the {@code Uni} will
     * never be completed.
     * <p>
     * The {@code Message} will not be sent to the channel until the {@code Uni} has been subscribed to:
     *
     * <pre>
     * emitter.send("a").subscribe().with(x -> {
     * });
     * </pre>
     *
     * @param payload the <em>thing</em> to send, must not be {@code null}
     * @return the {@code Uni}, that requires subscription to send the {@link Message}.
     * @throws IllegalStateException if the channel has been cancelled or terminated or if an overflow strategy of
     *         {@link OnOverflow.Strategy#THROW_EXCEPTION THROW_EXCEPTION} or {@link OnOverflow.Strategy#BUFFER BUFFER} is
     *         configured and the emitter overflows.
     */
    @CheckReturnValue
    Uni<Void> send(T payload);

    /**
     * Sends a payload to the channel.
     * <p>
     * A {@link Message} object will be created to hold the payload.
     * <p>
     * Execution will block waiting for the resulting {@code Message} to be acknowledged before returning.
     *
     * @param payload the <em>thing</em> to send, must not be {@code null}
     * @throws IllegalStateException if the channel has been cancelled or terminated or if an overflow strategy of
     *         {@link OnOverflow.Strategy#THROW_EXCEPTION THROW_EXCEPTION} or {@link OnOverflow.Strategy#BUFFER BUFFER} is
     *         configured and the emitter overflows.
     */
    void sendAndAwait(T payload);

    /**
     * Sends a payload to the channel without waiting for acknowledgement.
     * <p>
     * A {@link Message} object will be created to hold the payload.
     *
     * @param payload the <em>thing</em> to send, must not be {@code null}
     * @return the {@code Cancellable} from the subscribed {@code Uni}.
     * @throws IllegalStateException if the channel has been cancelled or terminated or if an overflow strategy of
     *         {@link OnOverflow.Strategy#THROW_EXCEPTION THROW_EXCEPTION} or {@link OnOverflow.Strategy#BUFFER BUFFER} is
     *         configured and the emitter overflows.
     */
    Cancellable sendAndForget(T payload);

    /**
     * Sends a message to the channel.
     *
     * @param <M> the <em>Message</em> type
     * @param msg the <em>Message</em> to send, must not be {@code null}
     * @deprecated use {{@link #sendMessageAndForget(Message)}}
     * @throws IllegalStateException if the channel has been cancelled or terminated or if an overflow strategy of
     *         {@link OnOverflow.Strategy#THROW_EXCEPTION THROW_EXCEPTION} or {@link OnOverflow.Strategy#BUFFER BUFFER} is
     *         configured and the emitter overflows.
     */
    @Deprecated
    <M extends Message<? extends T>> void send(M msg);

    /**
     * Sends a message to the channel.
     *
     * @param <M> the <em>Message</em> type
     * @param msg the <em>Message</em> to send, must not be {@code null}
     * @return the {@code Uni}, that requires subscription to send the {@link Message}.
     * @throws IllegalStateException if the channel has been cancelled or terminated or if an overflow strategy of
     *         {@link OnOverflow.Strategy#THROW_EXCEPTION THROW_EXCEPTION} or {@link OnOverflow.Strategy#BUFFER BUFFER} is
     *         configured and the emitter overflows.
     */
    <M extends Message<? extends T>> Uni<Void> sendMessage(M msg);

    /**
     * Sends a message to the channel.
     *
     * Execution will block waiting for the resulting {@code Message} to be acknowledged before returning.
     *
     * @param <M> the <em>Message</em> type
     * @param msg the <em>Message</em> to send, must not be {@code null}
     * @throws IllegalStateException if the channel has been cancelled or terminated or if an overflow strategy of
     *         {@link OnOverflow.Strategy#THROW_EXCEPTION THROW_EXCEPTION} or {@link OnOverflow.Strategy#BUFFER BUFFER} is
     *         configured and the emitter overflows.
     */
    <M extends Message<? extends T>> void sendMessageAndAwait(M msg);

    /**
     * Sends a message to the channel without waiting for acknowledgement.
     *
     * @param <M> the <em>Message</em> type
     * @param msg the <em>Message</em> to send, must not be {@code null}
     * @return the {@code Cancellable} from the subscribed {@code Uni}.
     * @throws IllegalStateException if the channel has been cancelled or terminated or if an overflow strategy of
     *         {@link OnOverflow.Strategy#THROW_EXCEPTION THROW_EXCEPTION} or {@link OnOverflow.Strategy#BUFFER BUFFER} is
     *         configured and the emitter overflows.
     */
    <M extends Message<? extends T>> Cancellable sendMessageAndForget(M msg);

    /**
     * Sends the completion event to the channel indicating that no other events will be sent afterward.
     */
    void complete();

    /**
     * Sends a failure event to the channel. No more events will be sent afterward.
     *
     * @param e the exception, must not be {@code null}
     */
    void error(Exception e);

    /**
     * @return {@code true} if the emitter has been terminated or the subscription cancelled.
     */
    boolean isCancelled();

    /**
     * @return {@code true} if one or more subscribers request messages from the corresponding channel where the emitter
     *         connects to,
     *         return {@code false} otherwise.
     */
    boolean hasRequests();
}
