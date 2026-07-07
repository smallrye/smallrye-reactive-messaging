package io.smallrye.reactive.messaging;

import java.util.Map;
import java.util.concurrent.Flow;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Factory for creating messaging channels from connector configurations.
 * <p>
 * Provides methods to create inbound (incoming) and outbound (outgoing) channels,
 * applying the decorator chain and registering them in the {@link ChannelRegistry}.
 */
public interface ChannelFactory {

    /**
     * Creates an inbound channel from the given connector configuration.
     * The channel is registered and the decorator chain is applied.
     *
     * @param channel the channel name
     * @param config the connector configuration (must contain a {@code connector} attribute)
     * @return a {@link Flow.Publisher} for consuming messages from this channel
     */
    Flow.Publisher<? extends Message<?>> incoming(String channel, Config config);

    /**
     * Creates an inbound channel where the registration name differs from the publisher channel name.
     * This is used for concurrency channels, where multiple indexed publishers (e.g. {@code "channel$1"},
     * {@code "channel$2"}) are registered under the same base channel name.
     *
     * @param registrationChannel the channel name used for registration in the {@link ChannelRegistry}
     * @param publisherChannel the channel name passed to the connector for publisher creation
     * @param config the connector configuration (must contain a {@code connector} attribute)
     * @return a {@link Flow.Publisher} for consuming messages from this channel
     */
    Flow.Publisher<? extends Message<?>> incoming(String registrationChannel, String publisherChannel, Config config);

    /**
     * Creates an outbound channel with an emitter for programmatic sending.
     *
     * @param channel the channel name
     * @param config the connector configuration (must contain a {@code connector} attribute)
     * @param emitterType the emitter type to create
     * @return an emitter for sending messages to this channel
     */
    <T> T outgoing(String channel, Config config, Class<T> emitterType);

    /**
     * Creates an outbound channel wired to a user-provided source publisher.
     * The source is subscribed to the outgoing connector's subscriber with
     * the decorator chain applied.
     *
     * @param channel the channel name
     * @param config the connector configuration (must contain a {@code connector} attribute)
     * @param source the source publisher providing messages for the outbound channel
     */
    void outgoing(String channel, Config config, Flow.Publisher<? extends Message<?>> source);

    /**
     * Creates an outbound channel and returns its subscriber.
     * The connector's subscriber is created and registered in the {@link ChannelRegistry}.
     * This is used during startup wiring when the framework connects an existing source
     * publisher to the outbound connector.
     *
     * @param channel the channel name
     * @param config the connector configuration (must contain a {@code connector} attribute)
     * @return a {@link Flow.Subscriber} for sending messages to this channel
     */
    Flow.Subscriber<? extends Message<?>> outgoing(String channel, Config config);

    /**
     * Creates an outbound channel with an emitter for programmatic sending,
     * from the given channel-specific configuration map.
     * Connector-wide defaults from the application configuration are used as fallback.
     *
     * @param channel the channel name
     * @param channelConfig flat map of channel-specific configuration properties
     * @param emitterType the emitter type to create
     * @return an emitter for sending messages to this channel
     */
    <T> T outgoing(String channel, Map<String, String> channelConfig, Class<T> emitterType);

    /**
     * Creates an outbound channel wired to a user-provided source publisher,
     * from the given channel-specific configuration map.
     * Connector-wide defaults from the application configuration are used as fallback.
     *
     * @param channel the channel name
     * @param channelConfig flat map of channel-specific configuration properties
     * @param source the source publisher providing messages for the outbound channel
     */
    void outgoing(String channel, Map<String, String> channelConfig, Flow.Publisher<? extends Message<?>> source);

    /**
     * Creates an outbound channel and returns its subscriber,
     * from the given channel-specific configuration map.
     * Connector-wide defaults from the application configuration are used as fallback.
     *
     * @param channel the channel name
     * @param channelConfig flat map of channel-specific configuration properties
     * @return a {@link Flow.Subscriber} for sending messages to this channel
     */
    Flow.Subscriber<? extends Message<?>> outgoing(String channel, Map<String, String> channelConfig);

    /**
     * Creates an inbound channel and returns a {@link ChannelBinding} for fluent subscription.
     * Combines {@link #incoming(String, Config)} and {@link #bind(Flow.Publisher, Class)} in one call.
     *
     * @param channel the channel name
     * @param config the connector configuration (must contain a {@code connector} attribute)
     * @param payloadType the target payload type for conversion
     * @param <T> the payload type
     * @return a {@link ChannelBinding} for subscribing to the channel
     */
    <T> ChannelBinding<T, T> incoming(String channel, Config config, Class<T> payloadType);

    /**
     * Creates an inbound channel from the given channel-specific configuration map,
     * and returns a {@link ChannelBinding} for fluent subscription.
     * Connector-wide defaults from the application configuration are used as fallback.
     *
     * @param channel the channel name
     * @param channelConfig flat map of channel-specific configuration properties
     * @param payloadType the target payload type for conversion
     * @param <T> the payload type
     * @return a {@link ChannelBinding} for subscribing to the channel
     */
    <T> ChannelBinding<T, T> incoming(String channel, Map<String, String> channelConfig, Class<T> payloadType);

    /**
     * Creates a {@link ChannelBinding} for subscribing to a channel publisher with
     * automatic payload conversion and ack/nack handling.
     * <p>
     * The payload is converted to the target type using available {@link MessageConverter}s.
     *
     * @param publisher the channel publisher, typically obtained from {@link #incoming(String, Config)}
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
     * @param publisher the channel publisher, typically obtained from {@link #incoming(String, Config)}
     * @return a {@link ChannelBinding} for subscribing to the channel
     */
    ChannelBinding<?, ?> bind(Flow.Publisher<? extends Message<?>> publisher);
}
