package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;

class FallbackConfigTest {

    private SmallRyeConfig baseConfig;
    private Map<String, Object> fallbacks;

    @BeforeEach
    public void setup() {
        Map<String, String> cfg = config();

        SmallRyeConfigBuilder builder = new SmallRyeConfigBuilder();
        builder.addDefaultSources();
        builder.withSources(new ConfigSource() {
            @Override
            public Map<String, String> getProperties() {
                return cfg;
            }

            @Override
            public Set<String> getPropertyNames() {
                return cfg.keySet();
            }

            @Override
            public int getOrdinal() {
                return ConfigSource.DEFAULT_ORDINAL;
            }

            @Override
            public String getValue(String s) {
                return cfg.get(s);
            }

            @Override
            public String getName() {
                return "test-config-source";
            }
        });
        baseConfig = builder.build();

        fallbacks = new HashMap<>();
        fallbacks.put("fallback.string", "fallback-value");
        fallbacks.put("fallback.int", 100);
        fallbacks.put("fallback.boolean", false);
        fallbacks.put("override.property", "from-fallback");
    }

    private static Map<String, String> config() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put("base.string", "base-value");
        cfg.put("base.int", "42");
        cfg.put("base.boolean", "true");
        cfg.put("override.property", "from-base");
        return cfg;
    }

    @Test
    public void testGetValueFromBaseConfig() {
        FallbackConfig config = new FallbackConfig(baseConfig, fallbacks);

        assertThat(config.getValue("base.string", String.class)).isEqualTo("base-value");
        assertThat(config.getValue("base.int", Integer.class)).isEqualTo(42);
        assertThat(config.getValue("base.boolean", Boolean.class)).isTrue();
    }

    @Test
    public void testGetValueFromFallback() {
        FallbackConfig config = new FallbackConfig(baseConfig, fallbacks);

        assertThat(config.getValue("fallback.string", String.class)).isEqualTo("fallback-value");
        assertThat(config.getValue("fallback.int", Integer.class)).isEqualTo(100);
        assertThat(config.getValue("fallback.boolean", Boolean.class)).isFalse();
    }

    @Test
    public void testBaseConfigTakesPrecedenceOverFallback() {
        FallbackConfig config = new FallbackConfig(baseConfig, fallbacks);

        // Base config should win over fallback
        assertThat(config.getValue("override.property", String.class)).isEqualTo("from-base");
    }

    @Test
    public void testGetOptionalValue() {
        FallbackConfig config = new FallbackConfig(baseConfig, fallbacks);

        assertThat(config.getOptionalValue("base.string", String.class)).hasValue("base-value");
        assertThat(config.getOptionalValue("fallback.string", String.class)).hasValue("fallback-value");
        assertThat(config.getOptionalValue("missing.property", String.class)).isEmpty();
    }

    @Test
    public void testGetValueThrowsForMissingProperty() {
        FallbackConfig config = new FallbackConfig(baseConfig, fallbacks);

        assertThatThrownBy(() -> config.getValue("missing.property", String.class))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessageContaining("missing.property");
    }

    @Test
    public void testTypeConversion() {
        Map<String, Object> conversionFallbacks = new HashMap<>();
        conversionFallbacks.put("string.to.int", "999");
        conversionFallbacks.put("string.to.boolean", "true");
        conversionFallbacks.put("string.to.long", "123456789");

        FallbackConfig config = new FallbackConfig(baseConfig, conversionFallbacks);

        assertThat(config.getValue("string.to.int", Integer.class)).isEqualTo(999);
        assertThat(config.getValue("string.to.boolean", Boolean.class)).isTrue();
        assertThat(config.getValue("string.to.long", Long.class)).isEqualTo(123456789L);
    }

    @Test
    public void testTypeConversionReturnsEmptyWhenNotPossible() {
        Map<String, Object> conversionFallbacks = new HashMap<>();
        conversionFallbacks.put("incompatible.type", new Object());

        FallbackConfig config = new FallbackConfig(baseConfig, conversionFallbacks);

        assertThat(config.getOptionalValue("incompatible.type", String.class)).isEmpty();
    }

    @Test
    public void testGetPropertyNames() {
        FallbackConfig config = new FallbackConfig(baseConfig, fallbacks);

        Iterable<String> propertyNames = config.getPropertyNames();

        assertThat(propertyNames)
                .contains("base.string", "base.int", "base.boolean")
                .contains("fallback.string", "fallback.int", "fallback.boolean")
                .contains("override.property");
    }

    @Test
    public void testGetConfigValue() {
        FallbackConfig config = new FallbackConfig(baseConfig, fallbacks);

        // ConfigValue only works for base config, not fallbacks
        ConfigValue baseValue = config.getConfigValue("base.string");
        assertThat(baseValue.getValue()).isEqualTo("base-value");
        assertThat(baseValue.getName()).isEqualTo("base.string");

        // Fallback values don't have ConfigValue metadata
        ConfigValue fallbackValue = config.getConfigValue("fallback.string");
        assertThat(fallbackValue.getValue()).isNull();
    }

    @Test
    public void testEmptyFallbacks() {
        FallbackConfig config = new FallbackConfig(baseConfig, new HashMap<>());

        assertThat(config.getValue("base.string", String.class)).isEqualTo("base-value");
        assertThat(config.getOptionalValue("fallback.string", String.class)).isEmpty();
    }

    @Test
    public void testGetConfigSources() {
        FallbackConfig config = new FallbackConfig(baseConfig, fallbacks);

        assertThat(config.getConfigSources()).isEqualTo(baseConfig.getConfigSources());
    }

    @Test
    public void testGetConverter() {
        FallbackConfig config = new FallbackConfig(baseConfig, fallbacks);

        assertThat(config.getConverter(Integer.class)).isPresent();
        assertThat(config.getConverter(String.class)).isPresent();
    }

    @Test
    public void testUnwrap() {
        FallbackConfig config = new FallbackConfig(baseConfig, fallbacks);

        assertThat(config.unwrap(SmallRyeConfig.class)).isEqualTo(baseConfig);
    }

    @Test
    public void testStringValueNotRequiringConverter() {
        Map<String, Object> stringFallbacks = new HashMap<>();
        stringFallbacks.put("plain.string", "value");

        FallbackConfig config = new FallbackConfig(baseConfig, stringFallbacks);

        assertThat(config.getOptionalValue("plain.string", String.class)).hasValue("value");
    }

    @Test
    public void testNullFallbackValue() {
        Map<String, Object> nullFallbacks = new HashMap<>();
        nullFallbacks.put("null.value", null);

        FallbackConfig config = new FallbackConfig(baseConfig, nullFallbacks);

        assertThat(config.getOptionalValue("null.value", String.class)).isEmpty();
    }

    @Test
    public void testMultipleFallbackLayers() {
        Map<String, Object> firstFallback = new HashMap<>();
        firstFallback.put("level1", "value1");
        firstFallback.put("shared", "level1-value");

        Map<String, Object> secondFallback = new HashMap<>();
        secondFallback.put("level2", "value2");
        secondFallback.put("shared", "level2-value");

        // First layer
        FallbackConfig layer1 = new FallbackConfig(baseConfig, firstFallback);
        // Second layer on top of first
        FallbackConfig layer2 = new FallbackConfig(layer1, secondFallback);

        assertThat(layer2.getValue("level1", String.class)).isEqualTo("value1");
        assertThat(layer2.getValue("level2", String.class)).isEqualTo("value2");
        // shared should come from first layer (layer1) since it's the base config for layer2
        assertThat(layer2.getValue("shared", String.class)).isEqualTo("level1-value");
    }

    @Test
    public void testFallbackWithComplexTypes() {
        Map<String, Object> complexFallbacks = new HashMap<>();
        complexFallbacks.put("double.value", 3.14);
        complexFallbacks.put("long.value", 9876543210L);

        FallbackConfig config = new FallbackConfig(baseConfig, complexFallbacks);

        assertThat(config.getValue("double.value", Double.class)).isEqualTo(3.14);
        assertThat(config.getValue("long.value", Long.class)).isEqualTo(9876543210L);
    }
}
