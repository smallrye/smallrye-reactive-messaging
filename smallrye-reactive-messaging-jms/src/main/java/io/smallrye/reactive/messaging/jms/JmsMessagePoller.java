package io.smallrye.reactive.messaging.jms;

import jakarta.jms.JMSConsumer;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;

/**
 * SPI for customizing how messages are polled from a JMS destination.
 * <p>
 * Custom implementations can be provided as CDI beans qualified with
 * {@link io.smallrye.common.annotation.Identifier @Identifier} and selected
 * via the {@code message-poller} channel configuration property.
 */
@Experimental("Experimental API")
public interface JmsMessagePoller {

    /**
     * Poll for a message. Implementations may block up to a provider-specific
     * timeout. Returning {@code null} signals that no message was available.
     *
     * @return a message wrapper, or {@code null} if no message is available
     * @throws Exception if polling fails
     */
    Message<jakarta.jms.Message> poll() throws Exception;

    /**
     * Called on channel shutdown.
     */
    default void close() {
    }

    /**
     * Factory for creating {@link JmsMessagePoller} instances.
     * <p>
     * The factory receives a pre-configured {@link JmsResourceHolder} that
     * manages the JMS context, destination, and consumer lifecycle including
     * reconnection on broker disconnection. The holder is {@code null} for
     * XA transaction mode, where each poll creates its own context.
     */
    interface Factory {
        JmsMessagePoller create(JmsConnectorIncomingConfiguration config, JmsResourceHolder<JMSConsumer> resourceHolder);
    }
}
