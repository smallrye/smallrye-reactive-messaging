package io.smallrye.reactive.messaging.providers.impl;

import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.INCOMING_PREFIX;
import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.OUTGOING_PREFIX;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.eclipse.microprofile.config.Config;

public final class Configs {

    private Configs() {
    }

    public static Config outgoing(Config config, String connectorName, String channel) {
        return new ConnectorConfig(OUTGOING_PREFIX, config, connectorName, channel);
    }

    public static Config incoming(Config config, String connectorName, String channel) {
        return new ConnectorConfig(INCOMING_PREFIX, config, connectorName, channel);
    }

    public static Config fallback(Config config, Map<String, Object> fallbacks) {
        if (fallbacks.isEmpty()) {
            return config;
        }
        return new FallbackConfig(config, fallbacks);
    }

    @SafeVarargs
    public static Config fallback(Config config, Map<String, Object>... fallbacks) {
        if (fallbacks.length == 0) {
            return config;
        }
        Map<String, Object> fall = new HashMap<>();
        for (Map<String, Object> fb : fallbacks) {
            fb.forEach(fall::putIfAbsent);
        }
        return fallback(config, fall);
    }

    public static Config prefixOverride(Config config, String prefix,
            Map<String, Function<OverrideConfig, Object>> overrides) {
        return prefix(prefix, functionsOverride(config, overrides));
    }

    public static Config prefix(String prefix, Config config) {
        return new PrefixedConfig(config, prefix);
    }

    public static Config override(Config config, Map<String, Object> overrides) {
        if (overrides.isEmpty()) {
            return config;
        }
        return new OverrideConfig(config, toFunctionsMap(overrides));
    }

    public static Config functionsOverride(Config config, Map<String, Function<OverrideConfig, Object>> overrides) {
        if (overrides.isEmpty()) {
            return config;
        }
        return new OverrideConfig(config, overrides);
    }

    private static Map<String, Function<OverrideConfig, Object>> toFunctionsMap(Map<String, Object> overrides) {
        return overrides.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> c -> e.getValue()));
    }
}
