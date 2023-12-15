package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
class OverrideConnectorConfigTest {

    private SmallRyeConfig overallConfig;
    private OverrideConnectorConfig config;
    private OverrideConnectorConfig config2;

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
        config = new OverrideConnectorConfig("mp.messaging.incoming.", overallConfig, "foo", "bar");
        config2 = new OverrideConnectorConfig("mp.messaging.incoming.", overallConfig, "foo", "bar",
                Map.of("attr1", c -> "some-other-value",
                        "attr2", c -> c.getOriginalValue("attr2", Integer.class).map(i -> i + 10)
                                .orElse(10)));
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
}
