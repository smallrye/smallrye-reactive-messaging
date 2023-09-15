package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;

@SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_FOO_ATTR", value = "new-value")
@SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_FOO_AT_TR", value = "another-value")
@SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_SOME_KEY", value = "another-value-from-connector")
@SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_SOME_OTHER_KEY", value = "another-value-other")
@SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_ATTR1", value = "should not be used")
@SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_ATTR3", value = "used")
@SetEnvironmentVariable(key = "mp_messaging_connector_some_connector_attr4", value = "used")
@SetEnvironmentVariable(key = "mp_messaging_connector_SOME_CONNECTOR_mixedcase", value = "should not be used")
@DisabledForJreRange(min = JRE.JAVA_17, disabledReason = "Environment cannot be modified on Java 17")
public class ConnectorConfigTest {

    private SmallRyeConfig overallConfig;
    private ConnectorConfig config;

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
        cfg.put("mp.messaging.connector.some-connector.key", "value");
        cfg.put("mp.messaging.connector.some-connector.some-key", "should not be used");
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
        config = new ConnectorConfig("mp.messaging.incoming.", overallConfig, "foo");
    }

    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_FOO_ATTR", value = "new-value")
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_FOO_AT_TR", value = "another-value")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_SOME_KEY", value = "another-value-from-connector")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_SOME_OTHER_KEY", value = "another-value-other")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_ATTR1", value = "should not be used")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_ATTR3", value = "used")
    @SetEnvironmentVariable(key = "mp_messaging_connector_some_connector_attr4", value = "used")
    @SetEnvironmentVariable(key = "mp_messaging_connector_SOME_CONNECTOR_mixedcase", value = "should not be used")
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
                        "at-tr", "key",
                        "SOME_KEY", "some-key", "SOME_OTHER_KEY", "ATTR3", "attr4", "channel-name");

        assertThat(config.getOptionalValue("connector", String.class)).hasValue("some-connector");
        assertThat(config.getOptionalValue("attr1", String.class)).hasValue("value");
        assertThat(config.getOptionalValue("attr2", Integer.class)).hasValue(23);
        assertThat(config.getOptionalValue("attr.2", String.class)).hasValue("test");
        assertThat(config.getOptionalValue("at-tr", String.class)).hasValue("another-value");
        assertThat(config.getOptionalValue("AT_TR", String.class)).hasValue("another-value");
        assertThat(config.getOptionalValue("some-key", String.class)).hasValue("another-value-from-connector");
        assertThat(config.getOptionalValue("SOME_KEY", String.class)).hasValue("another-value-from-connector");
        assertThat(config.getOptionalValue("key", String.class)).hasValue("value");
        assertThat(config.getOptionalValue("attr3", String.class)).hasValue("used");
        assertThat(config.getOptionalValue("ATTR3", String.class)).hasValue("used");
        assertThat(config.getOptionalValue("attr4", String.class)).hasValue("used");
    }

    @Test
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_ATTR3", value = "used")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_ATTR4", value = "used-2")
    public void testGetFromEnv() {
        // All uppercase value in env
        assertThat(config.getOptionalValue("ATTR3", String.class)).hasValue("used");
        // All lowercase value in env
        assertThat(config.getOptionalValue("ATTR4", String.class)).hasValue("used-2");
        // Mixed case value in env should not be found as it does not match the key we're looking for
        // either in its original casing, or after conversion to uppercase.
        assertThat(config.getOptionalValue("mixedcase", String.class)).isEmpty();
    }

    @Test
    public void testNameConversion() {
        assertThat(config.getValue("channel-name", String.class)).isEqualTo("foo");
        assertThat(config.getValue("connector", String.class)).isEqualTo("some-connector");
        assertThat(config.getValue("type", String.class)).isEqualTo("some-connector");

        assertThat(config.getValue("channel-name", boolean.class)).isEqualTo(false);
        assertThat(config.getValue("connector", boolean.class)).isEqualTo(false);
        assertThat(config.getValue("type", boolean.class)).isEqualTo(false);

        assertThatIllegalArgumentException().isThrownBy(() -> config.getValue("channel-name", int.class));
        assertThatIllegalArgumentException().isThrownBy(() -> config.getValue("connector", int.class));
        assertThatIllegalArgumentException().isThrownBy(() -> config.getValue("type", int.class));

        assertThatIllegalArgumentException().isThrownBy(() -> config.getOptionalValue("channel-name", int.class));
        assertThatIllegalArgumentException().isThrownBy(() -> config.getOptionalValue("connector", int.class));
        assertThatIllegalArgumentException().isThrownBy(() -> config.getOptionalValue("type", int.class));
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

}
