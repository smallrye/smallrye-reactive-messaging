package io.smallrye.reactive.messaging.providers.helpers;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;

import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;

public class ConfigUtils {

    private ConfigUtils() {
        // Avoid direct instantiation.
    }

    public static <T> T customize(Config channelConfig, Instance<ClientCustomizer<T>> customizers, T config) {
        T current = config;
        String channel = channelConfig.getValue(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, String.class);
        for (ClientCustomizer<T> customizer : CDIUtils.getSortedInstances(customizers)) {
            T applied = customizer.customize(channel, channelConfig, current);
            if (applied == null) {
                ProviderLogging.log.infof("Skipping config customizer %s to channel %s",
                        customizer.getClass().getName(), channel);
            } else {
                current = applied;
            }
        }
        return current;

    }
}
