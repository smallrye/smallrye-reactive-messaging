package io.smallrye.reactive.messaging;

import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.connector.IncomingConnectorConfiguration;
import io.smallrye.reactive.messaging.connector.OutgoingConnectorConfiguration;

/**
 * Factory for creating messaging channels from connector configurations.
 * <p>
 * Provides methods to create inbound (incoming) and outbound (outgoing) channels,
 * applying the decorator chain and registering them in the {@link ChannelRegistry}.
 */
public interface ChannelFactory {

    /**
     * Creates an inbound channel from the given connector configuration object
     * and returns a {@link ChannelBinding} for fluent subscription.
     * The channel name is extracted from the configuration.
     *
     * @param config the connector configuration
     * @param payloadType the target payload type for conversion
     * @param <T> the payload type
     * @return a {@link ChannelBinding} for subscribing to the channel
     */
    <T> ChannelBinding<T, T> incoming(IncomingConnectorConfiguration config, Class<T> payloadType);

    /**
     * Creates an outbound channel with an emitter for programmatic sending,
     * from the given connector configuration object.
     * The channel name is extracted from the configuration.
     *
     * @param config the connector configuration
     * @param emitterType the emitter type to create
     * @param <T> the emitter type
     * @return an emitter for sending messages to this channel
     */
    <T> T outgoing(OutgoingConnectorConfiguration config, Class<T> emitterType);

    /**
     * Creates an outbound channel wired to a user-provided source publisher,
     * from the given connector configuration object.
     * The channel name is extracted from the configuration.
     *
     * @param config the connector configuration
     * @param source the source publisher providing messages for the outbound channel
     */
    void outgoing(OutgoingConnectorConfiguration config, Flow.Publisher<? extends Message<?>> source);

    /**
     * Creates an outbound channel and returns its subscriber,
     * from the given connector configuration object.
     * The channel name is extracted from the configuration.
     *
     * @param config the connector configuration
     * @return a {@link Flow.Subscriber} for sending messages to this channel
     */
    Flow.Subscriber<? extends Message<?>> outgoing(OutgoingConnectorConfiguration config);

    /**
     * Creates a {@link ChannelBinding} for subscribing to a channel publisher with
     * automatic payload conversion and ack/nack handling.
     * <p>
     * The payload is converted to the target type using available {@link MessageConverter}s.
     *
     * @param publisher the channel publisher
     * @param payloadType the target payload type for conversion
     * @param <T> the payload type
     * @return a {@link ChannelBinding} for subscribing to the channel
     */
    <T> ChannelBinding<T, T> bind(Flow.Publisher<? extends Message<?>> publisher, Class<T> payloadType);

    /**
     * Creates a {@link ChannelBinding} for subscribing to a channel publisher with
     * automatic payload conversion and ack/nack handling.
     * <p>
     * No payload conversion is applied
     *
     * @param publisher the channel publisher
     * @return a {@link ChannelBinding} for subscribing to the channel
     */
    ChannelBinding<?, ?> bind(Flow.Publisher<? extends Message<?>> publisher);
}
