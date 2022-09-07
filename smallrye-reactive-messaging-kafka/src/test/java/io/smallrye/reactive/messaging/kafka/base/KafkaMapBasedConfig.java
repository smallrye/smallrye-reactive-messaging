package io.smallrye.reactive.messaging.kafka.base;

import java.util.Map;
import java.util.Objects;

import org.eclipse.microprofile.config.Config;
import org.jetbrains.annotations.NotNull;

import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * An implementation of {@link Config} based on a simple {@link Map}.
 * This class is just use to mock real configuration, so should only be used for tests.
 * <p>
 * Note that this implementation does not do any conversion, so you must pass the expected object instances.
 */
public class KafkaMapBasedConfig extends MapBasedConfig {

    private String prefix;
    private boolean tracing;

    KafkaMapBasedConfig(String prefix, boolean tracing) {
        this.tracing = tracing;
        withPrefix(prefix);
    }

    public KafkaMapBasedConfig withPrefix(String prefix) {
        Objects.requireNonNull(prefix);
        Object bootstrapServers = null;
        if (this.prefix != null) {
            bootstrapServers = this.get(getFullKey("bootstrap.servers"));
        }
        this.prefix = prefix;
        if (bootstrapServers != null) {
            this.put("bootstrap.servers", bootstrapServers);
        }
        this.put("connector", KafkaConnector.CONNECTOR_NAME);
        if (prefix.contains("incoming")) {
            this.put("graceful-shutdown", false);
        }
        if (!tracing) {
            this.put("tracing-enabled", false);
        }

        return this;
    }

    public KafkaMapBasedConfig withTracing(boolean tracing) {
        this.tracing = tracing;
        this.put("tracing-enabled", tracing);
        return this;
    }

    @NotNull
    public KafkaMapBasedConfig put(String key, Object value) {
        super.put(getFullKey(key), value);
        return this;
    }

    public KafkaMapBasedConfig with(String key, Object value) {
        this.put(key, value);
        return this;
    }

    private String getFullKey(String shortKey) {
        if (prefix.length() > 0) {
            return prefix + "." + shortKey;
        } else {
            return shortKey;
        }
    }

    public KafkaMapBasedConfig build(Object... keyOrValue) {
        String k = null;
        for (Object o : keyOrValue) {
            if (k == null) {
                if (o instanceof String) {
                    k = o.toString();
                } else {
                    throw new IllegalArgumentException("Expected " + o + " to be a String");
                }
            } else {
                put(k, o);
                k = null;
            }
        }
        if (k != null) {
            throw new IllegalArgumentException("Invalid number of parameters, last key " + k + " has no value");
        }
        return this;
    }

}
