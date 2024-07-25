package io.smallrye.reactive.messaging;

import jakarta.enterprise.inject.spi.Prioritized;

import org.eclipse.microprofile.config.Config;

/**
 * A customizer that can be used to modify the configuration used to create a messaging client.
 *
 * @param <T> the type of the configuration object
 */
public interface ClientCustomizer<T> extends Prioritized {

    /**
     * The default priority for config customizers.
     */
    int CLIENT_CONFIG_CUSTOMIZER_DEFAULT_PRIORITY = 100;

    /**
     * Customize the given configuration object.
     *
     * @param channel the channel name
     * @param channelConfig the channel configuration
     * @param config the configuration object
     * @return the modified configuration object, or {@code null} to skip this customizer
     */
    T customize(String channel, Config channelConfig, T config);

    @Override
    default int getPriority() {
        return CLIENT_CONFIG_CUSTOMIZER_DEFAULT_PRIORITY;
    }

}
