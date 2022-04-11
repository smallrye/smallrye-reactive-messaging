package io.smallrye.reactive.messaging.kafka.transactions;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;

/**
 * Emitter for sending messages to a Kafka channel in a transaction.
 * Send methods immediately return by calling the underlying Emitter.
 *
 * @param <T> payload type
 */
public interface TransactionalEmitter<T> {

    /**
     * Sends a message to the Kafka channel.
     * <p>
     * Immediately returns without waiting for the result by dispatching the message to the underlying Emitter.
     *
     * @param <M> the <em>Message</em> type
     * @param msg the <em>Message</em> to send, must not be {@code null}
     * @throws IllegalStateException if the channel has been cancelled or terminated or if an overflow strategy of
     *         {@link OnOverflow.Strategy#THROW_EXCEPTION THROW_EXCEPTION} or {@link OnOverflow.Strategy#BUFFER BUFFER} is
     *         configured and the emitter overflows.
     * @see io.smallrye.reactive.messaging.MutinyEmitter#send(Message)
     */
    <M extends Message<? extends T>> void send(M msg);

    /**
     * Sends a payload to the channel.
     * <p>
     * Immediately returns without waiting for the result by dispatching the message to the underlying Emitter.
     * <p>
     * A {@link Message} object will be created to hold the payload.
     *
     * @param payload the <em>thing</em> to send, must not be {@code null}.
     * @see io.smallrye.reactive.messaging.MutinyEmitter#send(Object)
     */
    void send(T payload);

    /**
     * Mark the current transaction for abort.
     */
    void markForAbort();

    /**
     * Is the current transaction marked for abort.
     */
    boolean isMarkedForAbort();
}
