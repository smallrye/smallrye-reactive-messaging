package io.smallrye.reactive.messaging.connector;

import org.eclipse.microprofile.config.Config;

/**
 * Base interface for connector channel configurations.
 * <p>
 * Generated connector configuration classes implement this interface,
 * enabling type-safe programmatic channel creation via {@link io.smallrye.reactive.messaging.ChannelFactory}.
 */
public interface ConnectorConfiguration {

    /**
     * @return the underlying configuration
     */
    Config config();

    /**
     * @return the channel name
     */
    String getChannel();
}
