package io.smallrye.reactive.messaging;

import jakarta.enterprise.inject.spi.Prioritized;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;

/**
 * Interceptor for outgoing messages on connector channels.
 * <p>
 * To register an outgoing interceptor, expose a managed bean, implementing this interface,
 * and qualified with {@code @Identifier} with the targeted channel name.
 * <p>
 * Only one interceptor is allowed to be bound for interception per outgoing channel.
 * When multiple interceptors are available, implementation should override the {@link #getPriority()} method.
 */
@Experimental("Smallrye-only feature")
public interface OutgoingInterceptor extends Prioritized {

    @Override
    default int getPriority() {
        return -1;
    }

    /**
     * Called before message transmission
     *
     * @param message message to send
     * @return the message to send, possibly mutated
     */
    default Message<?> onMessage(Message<?> message) {
        return message;
    }

    /**
     * Called after message acknowledgment
     *
     * @param message acknowledged message
     */
    void onMessageAck(Message<?> message);

    /**
     * Called after message negative-acknowledgement
     *
     * @param message message to negative-acknowledge
     * @param failure failure
     */
    void onMessageNack(Message<?> message, Throwable failure);
}
