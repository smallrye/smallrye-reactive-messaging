package io.smallrye.reactive.messaging;

import java.util.List;

import jakarta.enterprise.inject.spi.Prioritized;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Multi;

/**
 * SPI to allow extension of publishers (Multi) included in the final graph.
 *
 * {@code PublisherDecorator}s are invoked higher priority first (from the least value to the greatest).
 *
 * The decorator priority is obtained with the {@link #getPriority()} method.
 * The default priority is {@link #DEFAULT_PRIORITY}.
 */
@Experimental("SmallRye only feature")
public interface PublisherDecorator extends Prioritized {

    /**
     * Default priority
     */
    int DEFAULT_PRIORITY = 1000;

    /**
     * Name of the attribute set to {@code true} by the framework in the channel configuration passed to
     * {@link #decorate(Multi, List, Config)} when the decorated publisher feeds an outgoing channel, such as an
     * emitter (or another upstream publisher) bound to an outbound connector.
     * <p>
     * Such a channel has a connector configuration, but it is not a connector's incoming stream: the default
     * implementation of {@link #decorate(Multi, List, Config)} passes {@code isConnector = false} for it, as for any
     * emitter channel. See {@link #isConnectorChannel(Config)}.
     */
    String EMITTER_CHANNEL_ATTRIBUTE = "emitter-channel";

    /**
     * Decorate a Multi
     *
     * @param publisher the multi to decorate
     * @param channelName the name of the channel to which this publisher publishes
     * @param isConnector {@code true} if decorated channel is connector
     * @return the extended multi
     * @deprecated replaced with {@link #decorate(Multi, List, Config)}
     */
    @Deprecated(since = "4.12.0", forRemoval = true)
    default Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, String channelName,
            boolean isConnector) {
        return publisher;
    }

    /**
     * Decorate a Multi
     *
     * @param publisher the multi to decorate
     * @param channelName the list of channel names from which this publisher publishes
     * @param isConnector {@code true} if decorated channel is connector
     * @return the extended multi
     * @deprecated replaced with {@link #decorate(Multi, List, Config)}
     */
    @Deprecated(since = "4.37.0")
    default Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName,
            boolean isConnector) {
        return decorate(publisher, channelName.isEmpty() ? null : channelName.get(0), isConnector);
    }

    /**
     * Decorate a Multi
     *
     * @param publisher the multi to decorate
     * @param channelName the list of channel names from which this publisher publishes
     * @param channelConfig the channel configuration, or {@code null} if the channel has no connector configuration.
     *        For an emitter channel bound to an outbound connector, it carries the
     *        {@link #EMITTER_CHANNEL_ATTRIBUTE} attribute.
     * @return the extended multi
     */
    default Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName,
            Config channelConfig) {
        return decorate(publisher, channelName, isConnectorChannel(channelConfig));
    }

    /**
     * Whether the decorated publisher is a connector's incoming stream.
     *
     * @param channelConfig the channel configuration passed to {@link #decorate(Multi, List, Config)}, may be {@code null}
     * @return {@code true} if a configuration is present and it is not flagged with {@link #EMITTER_CHANNEL_ATTRIBUTE}
     */
    static boolean isConnectorChannel(Config channelConfig) {
        return channelConfig != null
                && !channelConfig.getOptionalValue(EMITTER_CHANNEL_ATTRIBUTE, Boolean.class).orElse(false);
    }

    @Override
    default int getPriority() {
        return DEFAULT_PRIORITY;
    }

}
