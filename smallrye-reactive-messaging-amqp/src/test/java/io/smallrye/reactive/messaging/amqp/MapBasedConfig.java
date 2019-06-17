package io.smallrye.reactive.messaging.amqp;

import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;

/**
 * An implementation of {@link Config} based on a simple {@link Map}.
 * This class is just use to mock real configuration, so should only be used for tests.
 * <p>
 * Note that this implementation does not do any conversion, so you must pass the expected object instances.
 */
public class MapBasedConfig implements Config {
    private final Map<String, Object> map;

    MapBasedConfig(Map<String, Object> map) {
        this.map = map;
    }

    @Override
    public <T> T getValue(String propertyName, Class<T> propertyType) {
        return getOptionalValue(propertyName, propertyType).orElseThrow(() -> new NoSuchElementException(propertyName));
    }

    @Override
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
        @SuppressWarnings("unchecked")
        T value = (T) map.get(propertyName);
        return Optional.ofNullable(value);
    }

    @Override
    public Iterable<String> getPropertyNames() {
        return map.keySet();
    }

    @Override
    public Iterable<ConfigSource> getConfigSources() {
        return Collections.emptyList();
    }
}
