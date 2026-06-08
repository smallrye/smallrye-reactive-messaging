package io.smallrye.reactive.messaging.test.common.config;

import org.eclipse.microprofile.config.ConfigProvider;

import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.config.SmallRyeConfigProviderResolver;

public class SmallRyeConfigTestUtil {

    private SmallRyeConfigTestUtil() {
    }

    public static void installConfig() {
        releaseConfig();
        SmallRyeConfigProviderResolver.instance().registerConfig(
                new SmallRyeConfigBuilder()
                        .addDefaultSources()
                        .addDefaultInterceptors()
                        .build(),
                Thread.currentThread().getContextClassLoader());
    }

    public static void releaseConfig() {
        try {
            SmallRyeConfigProviderResolver.instance()
                    .releaseConfig(ConfigProvider.getConfig(Thread.currentThread().getContextClassLoader()));
        } catch (IllegalArgumentException e) {
            // No config registered
        }
    }
}
