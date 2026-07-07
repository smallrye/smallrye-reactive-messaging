package io.smallrye.reactive.messaging.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

class MapBasedConfigTest {

    @Test
    void getStringValue() {
        MapBasedConfig config = new MapBasedConfig(Map.of("key", "value"));
        assertThat(config.getValue("key", String.class)).isEqualTo("value");
    }

    @Test
    void getBooleanValue() {
        MapBasedConfig config = new MapBasedConfig(Map.of("flag", "true"));
        assertThat(config.getValue("flag", Boolean.class)).isTrue();
    }

    @Test
    void getIntegerValue() {
        MapBasedConfig config = new MapBasedConfig(Map.of("count", "42"));
        assertThat(config.getValue("count", Integer.class)).isEqualTo(42);
    }

    @Test
    void getLongValue() {
        MapBasedConfig config = new MapBasedConfig(Map.of("big", "123456789"));
        assertThat(config.getValue("big", Long.class)).isEqualTo(123456789L);
    }

    @Test
    void getOptionalValuePresent() {
        MapBasedConfig config = new MapBasedConfig(Map.of("key", "value"));
        assertThat(config.getOptionalValue("key", String.class)).contains("value");
    }

    @Test
    void getOptionalValueMissing() {
        MapBasedConfig config = new MapBasedConfig(Map.of());
        assertThat(config.getOptionalValue("missing", String.class)).isEmpty();
    }

    @Test
    void getValueThrowsOnMissing() {
        MapBasedConfig config = new MapBasedConfig(Map.of());
        assertThatThrownBy(() -> config.getValue("missing", String.class))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void getPropertyNames() {
        MapBasedConfig config = new MapBasedConfig(Map.of("a", "1", "b", "2"));
        assertThat(config.getPropertyNames()).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void configIsImmutable() {
        var map = new java.util.HashMap<String, String>();
        map.put("key", "original");
        MapBasedConfig config = new MapBasedConfig(map);
        map.put("key", "modified");
        assertThat(config.getValue("key", String.class)).isEqualTo("original");
    }
}
