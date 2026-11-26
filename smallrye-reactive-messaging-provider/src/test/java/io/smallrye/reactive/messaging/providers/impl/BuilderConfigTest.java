package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

class BuilderConfigTest {

    @Test
    void getStringValue() {
        BuilderConfig config = new BuilderConfig(Map.of("key", "value"));
        assertThat(config.getValue("key", String.class)).isEqualTo("value");
    }

    @Test
    void getBooleanValue() {
        BuilderConfig config = new BuilderConfig(Map.of("flag", "true"));
        assertThat(config.getValue("flag", Boolean.class)).isTrue();
    }

    @Test
    void getIntegerValue() {
        BuilderConfig config = new BuilderConfig(Map.of("count", "42"));
        assertThat(config.getValue("count", Integer.class)).isEqualTo(42);
    }

    @Test
    void getLongValue() {
        BuilderConfig config = new BuilderConfig(Map.of("big", "123456789"));
        assertThat(config.getValue("big", Long.class)).isEqualTo(123456789L);
    }

    @Test
    void getOptionalValuePresent() {
        BuilderConfig config = new BuilderConfig(Map.of("key", "value"));
        assertThat(config.getOptionalValue("key", String.class)).contains("value");
    }

    @Test
    void getOptionalValueMissing() {
        BuilderConfig config = new BuilderConfig(Map.of());
        assertThat(config.getOptionalValue("missing", String.class)).isEmpty();
    }

    @Test
    void getValueThrowsOnMissing() {
        BuilderConfig config = new BuilderConfig(Map.of());
        assertThatThrownBy(() -> config.getValue("missing", String.class))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void getPropertyNames() {
        BuilderConfig config = new BuilderConfig(Map.of("a", "1", "b", "2"));
        assertThat(config.getPropertyNames()).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void configIsImmutable() {
        var map = new java.util.HashMap<String, String>();
        map.put("key", "original");
        BuilderConfig config = new BuilderConfig(map);
        map.put("key", "modified");
        assertThat(config.getValue("key", String.class)).isEqualTo("original");
    }
}
