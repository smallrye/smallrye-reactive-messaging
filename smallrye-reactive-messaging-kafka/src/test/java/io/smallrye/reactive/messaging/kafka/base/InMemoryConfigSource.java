package io.smallrye.reactive.messaging.kafka.base;

import java.util.Map;
import java.util.Set;

import org.eclipse.microprofile.config.spi.ConfigSource;

/**
 * An in-memory {@link ConfigSource} backed by a simple {@link Map}.
 * Used to register test configuration programmatically without writing to disk.
 */
public class InMemoryConfigSource implements ConfigSource {

    private final Map<String, String> properties;

    public InMemoryConfigSource(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public Set<String> getPropertyNames() {
        return properties.keySet();
    }

    @Override
    public String getValue(String propertyName) {
        return properties.get(propertyName);
    }

    @Override
    public String getName() {
        return "in-memory-test-config";
    }

    @Override
    public int getOrdinal() {
        return ConfigSource.DEFAULT_ORDINAL;
    }
}
