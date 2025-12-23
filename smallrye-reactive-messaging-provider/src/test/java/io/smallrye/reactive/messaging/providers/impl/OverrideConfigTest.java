package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.reactive.messaging.test.common.config.SetEnvironmentVariable;

@SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_FOO_ATTR", value = "new-value")
@SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_FOO_AT_TR", value = "another-value")
@SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_SOME_KEY", value = "another-value-from-connector")
@SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_SOME_OTHER_KEY", value = "another-value-other")
@SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_ATTR1", value = "should not be used")
@SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_ATTR3", value = "used")
@SetEnvironmentVariable(key = "mp_messaging_connector_some_connector_attr4", value = "used")
@SetEnvironmentVariable(key = "mp_messaging_connector_SOME_CONNECTOR_mixedcase", value = "used")
class OverrideConfigTest {

    private SmallRyeConfig overallConfig;
    private Config config;
    private Config config2;

    @BeforeEach
    public void createTestConfig() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put("mp.messaging.incoming.bar.connector", "some-connector");
        cfg.put("mp.messaging.incoming.bar.test", "test");
        cfg.put("mp.messaging.incoming.foo.connector", "some-connector");
        cfg.put("mp.messaging.incoming.foo.attr1", "value");
        cfg.put("mp.messaging.incoming.foo.attr2", "23");
        cfg.put("mp.messaging.incoming.foo.attr.2", "test");
        cfg.put("mp.messaging.incoming.foo.at-tr", "test");
        cfg.put("mp.messaging.incoming.foo.bar.qux", "value");
        cfg.put("mp.messaging.connector.some-connector.key", "value");
        cfg.put("mp.messaging.connector.some-connector.some-key", "should not be used");
        cfg.put("mp.messaging.connector.some-connector.bar.other-key", "another-value");
        cfg.put("MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_CAPSKEY", "should not be used");

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
                return "test";
            }
        });
        overallConfig = builder.build();
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", overallConfig, "some-connector", "foo");
        config = new PrefixedConfig(connectorConfig, "bar");
        config2 = new PrefixedConfig(new OverrideConfig(connectorConfig,
                Map.of("attr1", c -> "some-other-value",
                        "attr2", c -> c.getOriginalValue("attr2", Integer.class).map(i -> i + 10)
                                .orElse(10))),
                "bar");
    }

    @Test
    public void testGetConfigValue() {
        ConfigValue attr1 = config.getConfigValue("attr1");
        assertThat(attr1.getName()).isEqualTo("mp.messaging.incoming.foo.attr1");
        assertThat(attr1.getValue()).isEqualTo("value");
        assertThat(attr1.getRawValue()).isEqualTo("value");
        assertThat(attr1.getSourceName()).isEqualTo("test");
        assertThat(attr1.getSourceOrdinal()).isEqualTo(ConfigSource.DEFAULT_ORDINAL);

        ConfigValue attr3 = config.getConfigValue("attr3");
        assertThat(attr3.getName()).isEqualTo("mp.messaging.connector.some-connector.attr3"); // The value looked up in the overall config
        assertThat(attr3.getValue()).isEqualTo("used");
        assertThat(attr3.getRawValue()).isEqualTo("used");
        assertThat(attr3.getSourceOrdinal()).isEqualTo(300); // Env config source default ordinal

        ConfigValue channelName = config.getConfigValue("channel-name");
        assertThat(channelName.getName()).isEqualTo("channel-name");
        assertThat(channelName.getValue()).isEqualTo("foo");
        assertThat(channelName.getRawValue()).isEqualTo("foo");
        assertThat(channelName.getSourceName()).isEqualTo("ConnectorConfig internal");
        assertThat(channelName.getSourceOrdinal()).isEqualTo(0);
    }

    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_FOO_ATTR", value = "new-value")
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_FOO_AT_TR", value = "another-value")
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_FOO_BAR_KEY", value = "some-other-value")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_SOME_KEY", value = "another-value-from-connector")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_SOME_OTHER_KEY", value = "another-value-other")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_ATTR1", value = "should not be used")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_ATTR3", value = "used")
    @SetEnvironmentVariable(key = "mp_messaging_connector_some_connector_attr4", value = "used")
    @SetEnvironmentVariable(key = "mp_messaging_connector_SOME_CONNECTOR_mixedcase", value = "used")
    @Test
    public void testPropertyNames() {
        // Base config behaviour:
        // Even though looking up both properties would return the value of the environment variable,
        // both are included in the set returned from getPropertyNames.
        assertThat(overallConfig.getPropertyNames())
                .contains("mp.messaging.incoming.foo.at-tr",
                        "MP_MESSAGING_INCOMING_FOO_AT_TR");

        Iterable<String> names = config.getPropertyNames();
        assertThat(names)
                .containsExactlyInAnyOrder("connector", "ATTR1", "attr1", "attr2", "attr.2", "ATTR", "attr", "AT_TR",
                        "at-tr", "KEY", "qux", "other-key", "key",
                        "SOME_KEY", "some-key", "SOME_OTHER_KEY", "ATTR3", "attr4", "channel-name");

        assertThat(config.getOptionalValue("connector", String.class)).hasValue("some-connector");
        assertThat(config.getOptionalValue("attr1", String.class)).hasValue("value");
        assertThat(config.getOptionalValue("attr2", Integer.class)).hasValue(23);
        assertThat(config.getOptionalValue("attr.2", String.class)).hasValue("test");
        assertThat(config.getOptionalValue("at-tr", String.class)).hasValue("another-value");
        assertThat(config.getOptionalValue("AT_TR", String.class)).hasValue("another-value");
        assertThat(config.getOptionalValue("some-key", String.class)).hasValue("another-value-from-connector");
        assertThat(config.getOptionalValue("SOME_KEY", String.class)).hasValue("another-value-from-connector");
        assertThat(config.getOptionalValue("key", String.class)).hasValue("some-other-value");
        assertThat(config.getOptionalValue("attr3", String.class)).hasValue("used");
        assertThat(config.getOptionalValue("ATTR3", String.class)).hasValue("used");
        assertThat(config.getOptionalValue("attr4", String.class)).hasValue("used");
        assertThat(config.getOptionalValue("attr1", String.class)).hasValue("value");
        assertThat(config.getOptionalValue("qux", String.class)).hasValue("value");
        assertThat(config.getOptionalValue("bar.qux", String.class)).hasValue("value");
        assertThat(config.getOptionalValue("bar.other-key", String.class)).hasValue("another-value");
    }

    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_FOO_ATTR", value = "new-value")
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_FOO_AT_TR", value = "another-value")
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_FOO_BAR_KEY", value = "some-other-value")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_SOME_KEY", value = "another-value-from-connector")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_SOME_OTHER_KEY", value = "another-value-other")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_ATTR1", value = "should not be used")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_ATTR3", value = "used")
    @SetEnvironmentVariable(key = "mp_messaging_connector_some_connector_attr4", value = "used")
    @SetEnvironmentVariable(key = "mp_messaging_connector_SOME_CONNECTOR_mixedcase", value = "used")
    @Test
    public void testPropertyNamesOverriden() {
        // Base config behaviour:
        // Even though looking up both properties would return the value of the environment variable,
        // both are included in the set returned from getPropertyNames.
        assertThat(overallConfig.getPropertyNames())
                .contains("mp.messaging.incoming.foo.at-tr",
                        "MP_MESSAGING_INCOMING_FOO_AT_TR");

        Iterable<String> names = config2.getPropertyNames();
        assertThat(names)
                .containsExactlyInAnyOrder("connector", "ATTR1", "attr1", "attr2", "attr.2", "ATTR", "attr", "AT_TR",
                        "at-tr", "qux", "KEY", "other-key", "key",
                        "SOME_KEY", "some-key", "SOME_OTHER_KEY", "ATTR3", "attr4", "channel-name");

        assertThat(config2.getOptionalValue("connector", String.class)).hasValue("some-connector");
        assertThat(config2.getOptionalValue("attr1", String.class)).hasValue("some-other-value");
        assertThat(config2.getOptionalValue("attr2", Integer.class)).hasValue(33);
        assertThat(config2.getOptionalValue("attr.2", String.class)).hasValue("test");
        assertThat(config2.getOptionalValue("at-tr", String.class)).hasValue("another-value");
        assertThat(config2.getOptionalValue("AT_TR", String.class)).hasValue("another-value");
        assertThat(config2.getOptionalValue("some-key", String.class)).hasValue("another-value-from-connector");
        assertThat(config2.getOptionalValue("SOME_KEY", String.class)).hasValue("another-value-from-connector");
        assertThat(config2.getOptionalValue("key", String.class)).hasValue("some-other-value");
        assertThat(config2.getOptionalValue("attr3", String.class)).hasValue("used");
        assertThat(config2.getOptionalValue("ATTR3", String.class)).hasValue("used");
        assertThat(config2.getOptionalValue("attr4", String.class)).hasValue("used");
        assertThat(config2.getOptionalValue("qux", String.class)).hasValue("value");
        assertThat(config2.getOptionalValue("bar.qux", String.class)).hasValue("value");
        assertThat(config2.getOptionalValue("bar.other-key", String.class)).hasValue("another-value");
    }

    @Test
    public void testOverrideWithConcurrencyDelegation() {
        // Test OverrideConnectorConfig delegating to ConcurrencyConnectorConfig
        // This simulates the DLQ scenario where we need:
        // 1. Nested config support (mp.messaging.incoming.foo$0.dlq.topic)
        // 2. Fallback to base channel config (mp.messaging.incoming.foo.bootstrap.servers)

        Map<String, String> cfg = new HashMap<>();
        cfg.put("mp.messaging.incoming.foo.connector", "some-connector");
        cfg.put("mp.messaging.incoming.foo.base-property", "base-value");
        cfg.put("mp.messaging.incoming.foo$0.indexed-property", "indexed-value");
        cfg.put("mp.messaging.incoming.foo$0.dlq.nested-property", "nested-value");
        cfg.put("mp.messaging.incoming.foo.dlq.base-nested-property", "base-nested-value");

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
                return "test-concurrency";
            }
        });
        SmallRyeConfig config = builder.build();

        // Create ConcurrencyConnectorConfig as base
        ConcurrencyConnectorConfig concurrencyConfig = new ConcurrencyConnectorConfig(
                "mp.messaging.incoming.", config, "some-connector", "foo", 0);

        // Wrap it in OverrideConnectorConfig with nested channel "dlq"
        PrefixedConfig overrideConfig = new PrefixedConfig(concurrencyConfig, "dlq");

        assertThat(overrideConfig.getPropertyNames())
                .containsExactlyInAnyOrder("indexed-property", "nested-property", "base-nested-property",
                        "attr4", "ATTR", "AT_TR",
                        "base-property", "channel-name", "connector", "at.tr",
                        "ATTR3", "ATTR1", "attr", "SOME_OTHER_KEY", "SOME_KEY");

        // Verify channel name is the indexed one
        assertThat(overrideConfig.getValue("channel-name", String.class)).isEqualTo("foo$0");

        // Test 1: Nested property from indexed channel (mp.messaging.incoming.foo$0.dlq.nested-property)
        assertThat(overrideConfig.getOptionalValue("nested-property", String.class))
                .hasValue("nested-value");

        // Test 2: Nested property fallback to base channel (mp.messaging.incoming.foo.dlq.base-nested-property)
        assertThat(overrideConfig.getOptionalValue("base-nested-property", String.class))
                .hasValue("base-nested-value");

        // Test 3: Non-nested property from indexed channel (mp.messaging.incoming.foo$0.indexed-property)
        assertThat(overrideConfig.getOptionalValue("indexed-property", String.class))
                .hasValue("indexed-value");

        // Test 4: Non-nested property fallback to base channel (mp.messaging.incoming.foo.base-property)
        assertThat(overrideConfig.getOptionalValue("base-property", String.class))
                .hasValue("base-value");
    }

    @Test
    public void testOverrideFunctionReturningNull() {
        // When override function returns null, should fall back to base config value
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", overallConfig, "some-connector",
                "foo");

        OverrideConfig overrideConfig = new OverrideConfig(connectorConfig,
                Map.of("attr1", c -> null,
                        "attr2", c -> null));

        // Should fall back to base config values
        assertThat(overrideConfig.getValue("attr1", String.class)).isEqualTo("value");
        assertThat(overrideConfig.getValue("attr2", Integer.class)).isEqualTo(23);
    }

    @Test
    public void testOverrideFunctionAccessingNonExistentOriginalValue() {
        // Override function tries to access non-existent key via getOriginalValue
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", overallConfig, "some-connector",
                "foo");

        OverrideConfig overrideConfig = new OverrideConfig(connectorConfig,
                Map.of("new-property", c -> c.getOriginalValue("non-existent-key", String.class)
                        .map(v -> v + "-modified")
                        .orElse("default-value")));

        // Should get the default value from the function
        assertThat(overrideConfig.getValue("new-property", String.class)).isEqualTo("default-value");
    }

    @Test
    public void testOverrideFunctionWithComplexTransformation() {
        // Test override function that does complex transformations on original value
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", overallConfig, "some-connector",
                "foo");

        OverrideConfig overrideConfig = new OverrideConfig(connectorConfig,
                Map.of(
                        // Triple the integer value
                        "attr2", c -> c.getOriginalValue("attr2", Integer.class).map(i -> i * 3).orElse(0),
                        // Transform string by appending channel name
                        "attr1", c -> c.getOriginalValue("attr1", String.class).orElse("") + "-"
                                + c.getOriginalValue("channel-name", String.class).orElse("unknown"),
                        // Combine multiple values
                        "combined", c -> c.getOriginalValue("attr1", String.class).orElse("")
                                + ":" + c.getOriginalValue("attr2", Integer.class).map(String::valueOf).orElse("0")));

        assertThat(overrideConfig.getValue("attr2", Integer.class)).isEqualTo(69); // 23 * 3
        assertThat(overrideConfig.getValue("attr1", String.class)).isEqualTo("value-foo");
        assertThat(overrideConfig.getValue("combined", String.class)).isEqualTo("value:23");
    }

    @Test
    public void testChainingMultipleOverrideConfigs() {
        // Test chaining multiple OverrideConfig instances
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", overallConfig, "some-connector",
                "foo");

        // First override layer
        OverrideConfig firstOverride = new OverrideConfig(connectorConfig,
                Map.of("attr1", c -> "first-override",
                        "attr2", c -> 100));

        // Second override layer on top of first
        OverrideConfig secondOverride = new OverrideConfig(firstOverride,
                Map.of("attr1", c -> c.getOriginalValue("attr1", String.class).orElse("") + "-second",
                        "attr3", c -> "new-value"));

        // Third override layer
        OverrideConfig thirdOverride = new OverrideConfig(secondOverride,
                Map.of("attr2", c -> c.getOriginalValue("attr2", Integer.class).map(i -> i + 50).orElse(0)));

        // attr1 should be modified by second override (first-override-second)
        assertThat(thirdOverride.getValue("attr1", String.class)).isEqualTo("first-override-second");

        // attr2 should be modified by third override (100 + 50 = 150)
        assertThat(thirdOverride.getValue("attr2", Integer.class)).isEqualTo(150);

        // attr3 should come from second override
        assertThat(thirdOverride.getValue("attr3", String.class)).isEqualTo("new-value");

        // Other properties should still be accessible from base
        assertThat(thirdOverride.getOptionalValue("bar.qux", String.class)).hasValue("value");
    }

    @Test
    public void testOverrideConfigWithEmptyOverrides() {
        // OverrideConfig with empty overrides map should behave like base config
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", overallConfig, "some-connector",
                "foo");

        OverrideConfig overrideConfig = new OverrideConfig(connectorConfig, Map.of());

        assertThat(overrideConfig.getValue("attr1", String.class)).isEqualTo("value");
        assertThat(overrideConfig.getValue("attr2", Integer.class)).isEqualTo(23);
        assertThat(overrideConfig.getOptionalValue("bar.qux", String.class)).hasValue("value");
    }

    @Test
    public void testOverrideFunctionReturningDifferentType() {
        // Test override function that returns a value requiring type conversion
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", overallConfig, "some-connector",
                "foo");

        OverrideConfig overrideConfig = new OverrideConfig(connectorConfig,
                Map.of(
                        // Return string that will be converted to Integer
                        "attr2", c -> "999",
                        // Return integer that will be converted to String
                        "attr1", c -> 42));

        // Type conversion should work
        assertThat(overrideConfig.getValue("attr2", Integer.class)).isEqualTo(999);
        assertThat(overrideConfig.getValue("attr1", Integer.class)).isEqualTo(42);
        Assertions.assertThatThrownBy(() -> overrideConfig.getValue("attr1", String.class))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testOverrideConfigPropertyNames() {
        // Override config should include properties from both overrides and base config
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", overallConfig, "some-connector",
                "foo");

        OverrideConfig overrideConfig = new OverrideConfig(connectorConfig,
                Map.of("new-property", c -> "new-value",
                        "another-property", c -> "another-value"));

        Iterable<String> propertyNames = overrideConfig.getPropertyNames();

        // Should include original properties
        assertThat(propertyNames).contains("attr1", "attr2", "bar.qux");

        // Should include new properties from overrides
        assertThat(propertyNames).contains("new-property", "another-property");
    }

    @Test
    public void testOverrideConfigGetConfigValue() {
        // Test that getConfigValue works correctly with override config
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", overallConfig, "some-connector",
                "foo");

        OverrideConfig overrideConfig = new OverrideConfig(connectorConfig,
                Map.of("attr1", c -> "overridden-value"));

        // Overridden property - ConfigValue should reflect the override
        ConfigValue attr1 = overrideConfig.getConfigValue("attr1");
        assertThat(attr1.getValue()).isEqualTo("overridden-value");
        assertThat(attr1.getName()).isEqualTo("attr1");

        // Non-overridden property - should get original ConfigValue
        ConfigValue attr2 = overrideConfig.getConfigValue("attr2");
        assertThat(attr2.getValue()).isEqualTo("23");
        assertThat(attr2.getName()).isEqualTo("mp.messaging.incoming.foo.attr2");
    }

    @Test
    public void testOverrideWithPrefixedConfigCombination() {
        // Test OverrideConfig combined with PrefixedConfig
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", overallConfig, "some-connector",
                "foo");

        // Apply override first, then prefix
        OverrideConfig overrideConfig = new OverrideConfig(connectorConfig,
                Map.of("bar.custom-key", c -> "custom-value",
                        "bar.qux", c -> "overridden-qux"));

        PrefixedConfig prefixedConfig = new PrefixedConfig(overrideConfig, "bar");

        // Should get overridden value through prefix
        assertThat(prefixedConfig.getValue("custom-key", String.class)).isEqualTo("custom-value");
        assertThat(prefixedConfig.getValue("qux", String.class)).isEqualTo("overridden-qux");

        // Non-overridden bar properties should still work
        assertThat(prefixedConfig.getOptionalValue("other-key", String.class)).hasValue("another-value");
    }

    @Test
    public void testOverrideFunctionCanAccessNestedPrefixedProperties() {
        // Test that override function can access properties with nested prefixes
        ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", overallConfig, "some-connector",
                "foo");

        OverrideConfig overrideConfig = new OverrideConfig(connectorConfig,
                Map.of("computed", c -> c.getOriginalValue("bar.qux", String.class)
                        .map(v -> "computed-from-" + v)
                        .orElse("missing")));

        assertThat(overrideConfig.getValue("computed", String.class)).isEqualTo("computed-from-value");
    }
}
