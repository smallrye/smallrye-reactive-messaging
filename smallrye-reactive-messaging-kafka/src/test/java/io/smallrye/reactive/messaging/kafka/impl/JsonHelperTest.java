package io.smallrye.reactive.messaging.kafka.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.reactive.messaging.providers.impl.ConnectorConfig;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;

class JsonHelperTest {

    @Test
    public void test() {
        MapBasedConfig config = new MapBasedConfig()
                .with("bootstrap.servers", "not-important")
                .with("key", "value")
                .with("int", 10)
                .with("double", 23.4)
                .with("trueasstring", "true")
                .with("falseasstring", "false")
                .with("boolean.t", true)
                .with("boolean.f", false)
                .with("FOO_BAR", "value")
                .with("dataFormat", "DF")
                .with("UP", "up");

        JsonObject object = JsonHelper.asJsonObject(config);
        assertThat(object.getString("key")).isEqualTo("value");
        assertThat(object.getString("dataFormat")).isEqualTo("DF");
        assertThat(object.getInteger("int")).isEqualTo(10);
        assertThat(object.getDouble("double")).isEqualTo(23.4);
        assertThat(object.getBoolean("trueasstring")).isTrue();
        assertThat(object.getBoolean("falseasstring")).isFalse();
        assertThat(object.getString("foo.bar")).isEqualTo("value");
        assertThat(object.getString("FOO_BAR")).isNull();
        assertThat(object.getString("UP")).isNull();
        assertThat(object.getString("up")).isEqualTo("up");

        assertThat(object.getString("bootstrap.servers")).isEqualTo("not-important");

        assertThat(object.getBoolean("boolean.t")).isTrue();
        assertThat(object.getBoolean("boolean.f")).isFalse();

    }

    @Test
    @DisabledForJreRange(min = JRE.JAVA_17, disabledReason = "Environment cannot be modified on Java 17")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SMALLRYE_KAFKA_BOOTSTRAP_SERVERS", value = "testServers")
    public void testConnectorConfigFromEnv() {
        JsonObject object = JsonHelper.asJsonObject(createTestConfig());
        assertThat(object.getString("bootstrap.servers")).isEqualTo("testServers");
    }

    @Test
    @DisabledForJreRange(min = JRE.JAVA_17, disabledReason = "Environment cannot be modified on Java 17")
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_TESTCHANNEL_BOOTSTRAP_SERVERS", value = "testServers")
    public void testChannelConfigFromEnv() {
        JsonObject object = JsonHelper.asJsonObject(createTestConfig());
        assertThat(object.getString("bootstrap.servers")).isEqualTo("testServers");
    }

    @Test
    @DisabledForJreRange(min = JRE.JAVA_17, disabledReason = "Environment cannot be modified on Java 17")
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_TESTCHANNEL_FOO_BAR", value = "a")
    public void testSysPropsOverrides() {
        Map<String, String> properties = Collections.singletonMap("mp.messaging.incoming.testChannel.foo.bar", "b");
        ConfigSource highOrdinalSource = getConfigSourceFromMap(properties, 400);

        JsonObject object = JsonHelper.asJsonObject(createTestConfig(highOrdinalSource));
        assertThat(object.getString("foo.bar")).isEqualTo("b");
        assertThat(object.getValue("FOO_BAR")).isNull();
    }

    @Test
    @DisabledForJreRange(min = JRE.JAVA_17, disabledReason = "Environment cannot be modified on Java 17")
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_TESTCHANNEL_FOO_BAR", value = "a")
    public void testSysPropsCaseSensitive() {
        Map<String, String> properties = Collections.singletonMap("mp.messaging.incoming.testChannel.FOO_BAR", "b");
        ConfigSource highOrdinalSource = getConfigSourceFromMap(properties, 400);

        JsonObject object = JsonHelper.asJsonObject(createTestConfig(highOrdinalSource));
        assertThat(object.getString("foo.bar")).isEqualTo("a");
        assertThat(object.getValue("FOO_BAR")).isNull();
    }

    @Test
    @DisabledForJreRange(min = JRE.JAVA_17, disabledReason = "Environment cannot be modified on Java 17")
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_TESTCHANNEL_FOO_BAR", value = "a")
    public void testEnvOverrides() {
        Map<String, String> extraProperties = new HashMap<>();
        extraProperties.put("mp.messaging.incoming.testChannel.foo.bar", "b");
        extraProperties.put("mp.messaging.incoming.testChannel.baz.qux", "c");
        ConfigSource lowOrdinalSource = getConfigSourceFromMap(extraProperties, 100); // Ordinal 100, lower than environment variables

        JsonObject object = JsonHelper.asJsonObject(createTestConfig(lowOrdinalSource));
        assertThat(object.getString("foo.bar")).isEqualTo("a");
        assertThat(object.getString("baz.qux")).isEqualTo("c");
    }

    @Test
    @DisabledForJreRange(min = JRE.JAVA_17, disabledReason = "Environment cannot be modified on Java 17")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SMALLRYE_KAFKA_FOO_BAR", value = "a")
    public void testConnectorSysPropsOverrides() {
        Map<String, String> properties = Collections.singletonMap("mp.messaging.connector.smallrye-kafka.foo.bar", "b");
        ConfigSource highOrdinalSource = getConfigSourceFromMap(properties, 400);

        JsonObject object = JsonHelper.asJsonObject(createTestConfig(highOrdinalSource));
        assertThat(object.getString("foo.bar")).isEqualTo("b");
        assertThat(object.getValue("FOO_BAR")).isNull();
    }

    @Test
    @DisabledForJreRange(min = JRE.JAVA_17, disabledReason = "Environment cannot be modified on Java 17")
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_TESTCHANNEL_FOO_BAR", value = "a")
    public void testChannelOverridesConnector() {
        Map<String, String> properties = Collections.singletonMap("mp.messaging.connector.smallrye-kafka.foo.bar", "b");
        ConfigSource highOrdinalSource = getConfigSourceFromMap(properties, 400);

        JsonObject object = JsonHelper.asJsonObject(createTestConfig(highOrdinalSource));
        assertThat(object.getString("foo.bar")).isEqualTo("a");
        assertThat(object.getValue("FOO_BAR")).isNull();

        // Sys props are higher ordinal than env vars, but we always prefer channel-scoped config to connector-scoped config
    }

    private ConnectorConfig createTestConfig(ConfigSource... extraSources) {
        Map<String, String> channelConnector = Collections.singletonMap("mp.messaging.incoming.testChannel.connector",
                "smallrye-kafka");
        SmallRyeConfig config = new SmallRyeConfigBuilder()
                .addDefaultSources()
                .withSources(getConfigSourceFromMap(channelConnector, 0))
                .withSources(extraSources)
                .build();
        return new TestConnectorConfig(config);
    }

    private ConfigSource getConfigSourceFromMap(Map<String, String> configMap, int ordinal) {
        return new ConfigSource() {

            @Override
            public String getValue(String propertyName) {
                return configMap.get(propertyName);
            }

            @Override
            public Set<String> getPropertyNames() {
                return configMap.keySet();
            }

            @Override
            public Map<String, String> getProperties() {
                return configMap;
            }

            @Override
            public int getOrdinal() {
                return ordinal;
            }

            @Override
            public String getName() {
                return "TestConfigSource" + ordinal;
            }
        };
    }

    /**
     * Test class to allow calling protected constructor
     */
    private static class TestConnectorConfig extends ConnectorConfig {
        public TestConnectorConfig(Config overall) {
            super("mp.messaging.incoming.", overall, "testChannel");
        }
    }

}
