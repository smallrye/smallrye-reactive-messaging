package io.smallrye.reactive.messaging.tck;

import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.BeforeBeanDiscovery;
import jakarta.enterprise.inject.spi.Extension;

import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.config.SmallRyeConfigProviderResolver;

public class SmallRyeConfigRegistrationExtension implements Extension {

    void registerConfig(@Observes BeforeBeanDiscovery event) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        SmallRyeConfigProviderResolver resolver = (SmallRyeConfigProviderResolver) SmallRyeConfigProviderResolver
                .instance();
        try {
            resolver.releaseConfig(resolver.getConfig(cl));
        } catch (IllegalArgumentException e) {
            // No config registered yet
        }
        resolver.registerConfig(
                new SmallRyeConfigBuilder()
                        .addDefaultSources()
                        .addDefaultInterceptors()
                        .build(),
                cl);
    }
}
