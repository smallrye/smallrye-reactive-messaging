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
import io.smallrye.reactive.messaging.test.common.config.SetEnvironmentVariable;

@SetEnvironmentVariable(key = "PREFIX_KEY1", value = "env-value1")
@SetEnvironmentVariable(key = "PREFIX_KEY2", value = "env-value2")
@SetEnvironmentVariable(key = "DLQ_TOPIC", value = "dlq-topic-from-env")
class PrefixedConfigTest {

    private SmallRyeConfig baseConfig;

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
    }

    private static Map<String, String> config() {
        Map<String, String> cfg = new HashMap<>();
        // Base properties
        cfg.put("base.property", "base-value");
        cfg.put("shared.property", "base-shared");

        // Prefixed properties
        cfg.put("prefix.key1", "prefixed-value1");
        cfg.put("prefix.key2", "prefixed-value2");
        cfg.put("prefix.nested.key", "nested-value");
        cfg.put("prefix.shared.property", "prefix-shared");

        // DLQ scenario
        cfg.put("topic", "main-topic");
        cfg.put("bootstrap.servers", "localhost:9092");
        cfg.put("dlq.topic", "dlq-topic");
        cfg.put("dlq.max.retries", "5");

        // Integer values
        cfg.put("prefix.int.value", "42");
        cfg.put("int.value", "10");
        return cfg;
    }

    @Test
    public void testPrefixedLookupTakesPrecedence() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "prefix");

        // Environment variable PREFIX_KEY1 has higher priority than config file
        // So it will return env-value1 instead of prefixed-value1
        assertThat(config.getValue("key1", String.class)).isEqualTo("env-value1");
        assertThat(config.getValue("key2", String.class)).isEqualTo("env-value2");
    }

    @Test
    public void testFallbackToBaseConfig() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "prefix");

        // Should fall back to base config when prefix not found
        assertThat(config.getValue("base.property", String.class)).isEqualTo("base-value");
    }

    @Test
    public void testPrefixedTakesPrecedenceOverBase() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "prefix");

        // Prefixed property should win over base
        assertThat(config.getValue("shared.property", String.class)).isEqualTo("prefix-shared");
    }

    @Test
    public void testNestedPrefixedKey() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "prefix");

        assertThat(config.getValue("nested.key", String.class)).isEqualTo("nested-value");
    }

    @Test
    public void testDLQScenario() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "dlq");

        // Environment variable DLQ_TOPIC has higher priority
        assertThat(config.getValue("topic", String.class)).isEqualTo("dlq-topic-from-env");
        assertThat(config.getValue("max.retries", Integer.class)).isEqualTo(5);

        // Fall back to main config
        assertThat(config.getValue("bootstrap.servers", String.class)).isEqualTo("localhost:9092");
    }

    @Test
    public void testGetOptionalValue() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "prefix");

        // Environment variable PREFIX_KEY1 has higher priority
        assertThat(config.getOptionalValue("key1", String.class)).hasValue("env-value1");
        assertThat(config.getOptionalValue("base.property", String.class)).hasValue("base-value");
        assertThat(config.getOptionalValue("missing.key", String.class)).isEmpty();
    }

    @Test
    public void testGetValueThrowsForMissingProperty() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "prefix");

        assertThatThrownBy(() -> config.getValue("missing.property", String.class))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessageContaining("missing.property");
    }

    @Test
    public void testTypeConversion() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "prefix");

        assertThat(config.getValue("int.value", Integer.class)).isEqualTo(42);
    }

    @Test
    public void testGetConfigValue() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "prefix");

        // Environment variable PREFIX_KEY1 has higher priority
        // But ConfigValue name is the looked-up key "prefix.key1"
        ConfigValue prefixedValue = config.getConfigValue("key1");
        assertThat(prefixedValue.getValue()).isEqualTo("env-value1");
        assertThat(prefixedValue.getName()).isEqualTo("prefix.key1");

        ConfigValue baseValue = config.getConfigValue("base.property");
        assertThat(baseValue.getValue()).isEqualTo("base-value");
        assertThat(baseValue.getName()).isEqualTo("base.property");
    }

    @Test
    public void testGetPropertyNames() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "prefix");

        Iterable<String> propertyNames = config.getPropertyNames();

        // Should include properties with prefix stripped
        assertThat(propertyNames)
                .contains("key1", "key2", "nested.key", "shared.property")
                // Should also include base properties
                .contains("base.property", "topic", "bootstrap.servers");
    }

    @Test
    public void testGetPropertyNamesWithEnvironmentVariables() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "prefix");

        Iterable<String> propertyNames = config.getPropertyNames();

        // Environment variables should be included with prefix stripped
        assertThat(propertyNames).contains("KEY1", "KEY2");
    }

    @Test
    public void testGetPropertyNamesForDLQPrefix() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "dlq");

        Iterable<String> propertyNames = config.getPropertyNames();

        // DLQ-specific properties with prefix stripped
        assertThat(propertyNames)
                .contains("topic", "max.retries")
                // Base properties
                .contains("base.property", "bootstrap.servers");

        // Should include env variable
        assertThat(propertyNames).contains("TOPIC");
    }

    @Test
    public void testNullPrefix() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, null);

        // With null prefix, should just use base config
        assertThat(config.getValue("base.property", String.class)).isEqualTo("base-value");
        // Environment variable PREFIX_KEY1 takes precedence over config file
        assertThat(config.getOptionalValue("prefix.key1", String.class)).hasValue("env-value1");
    }

    @Test
    public void testEmptyPrefix() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "");

        // With empty prefix, should just use base config
        assertThat(config.getValue("base.property", String.class)).isEqualTo("base-value");
    }

    @Test
    public void testGetConfigSources() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "prefix");

        assertThat(config.getConfigSources()).isEqualTo(baseConfig.getConfigSources());
    }

    @Test
    public void testGetConverter() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "prefix");

        assertThat(config.getConverter(Integer.class)).isPresent();
        assertThat(config.getConverter(String.class)).isPresent();
    }

    @Test
    public void testUnwrap() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "prefix");

        assertThat(config.unwrap(SmallRyeConfig.class)).isEqualTo(baseConfig);
    }

    @Test
    public void testNestedPrefixedConfig() {
        // First level prefix
        PrefixedConfig level1 = new PrefixedConfig(baseConfig, "prefix");
        // Second level prefix
        PrefixedConfig level2 = new PrefixedConfig(level1, "nested");

        // Should lookup: prefix.nested.key -> not found
        // Then: prefix.key -> found!
        // Since level1 already has "prefix", level2 looks for "nested.key" in level1
        // which translates to "prefix.nested.key" in base config
        assertThat(level2.getValue("key", String.class)).isEqualTo("nested-value");
    }

    @Test
    public void testMultiplePrefixLevels() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put("a.b.c", "abc-value");
        cfg.put("a.b", "ab-value");
        cfg.put("a", "a-value");

        SmallRyeConfigBuilder builder = new SmallRyeConfigBuilder();
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
                return "test";
            }
        });
        SmallRyeConfig config = builder.build();

        PrefixedConfig prefixA = new PrefixedConfig(config, "a");
        assertThat(prefixA.getValue("b.c", String.class)).isEqualTo("abc-value");
        assertThat(prefixA.getValue("b", String.class)).isEqualTo("ab-value");

        PrefixedConfig prefixAB = new PrefixedConfig(prefixA, "b");
        assertThat(prefixAB.getValue("c", String.class)).isEqualTo("abc-value");
    }

    @Test
    public void testStaticPrefixedMethod() {
        PrefixedConfig config = PrefixedConfig.prefixed("prefix", baseConfig);

        // Environment variable PREFIX_KEY1 has higher priority
        assertThat(config.getValue("key1", String.class)).isEqualTo("env-value1");
        assertThat(config.getValue("base.property", String.class)).isEqualTo("base-value");
    }

    @Test
    public void testConfigValueWithNullRawValue() {
        PrefixedConfig config = new PrefixedConfig(baseConfig, "prefix");

        ConfigValue missingValue = config.getConfigValue("completely.missing.key");
        assertThat(missingValue.getRawValue()).isNull();
    }

    @Test
    public void testAlphaNumericConversion() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put("my-prefix.my-key", "value1");
        cfg.put("MY_PREFIX_MY_KEY", "env-value");
        cfg.put("my_prefix_another_key", "value2");

        SmallRyeConfigBuilder builder = new SmallRyeConfigBuilder();
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
                return "test";
            }
        });
        SmallRyeConfig config = builder.build();

        PrefixedConfig prefixedConfig = new PrefixedConfig(config, "my-prefix");

        Iterable<String> propertyNames = prefixedConfig.getPropertyNames();

        // Should strip prefix and include alphanumeric variants
        assertThat(propertyNames).contains("my-key", "MY_KEY", "another_key");
    }
}
