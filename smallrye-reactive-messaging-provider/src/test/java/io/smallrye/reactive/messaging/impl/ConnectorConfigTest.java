package io.smallrye.reactive.messaging.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;

public class ConnectorConfigTest {

    @Test
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_FOO_ATTR", value = "new-value")
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_FOO_AT_TR", value = "another-value")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_SOME_KEY", value = "another-value-from-connector")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_SOME_OTHER_KEY", value = "another-value-other")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_ATTR1", value = "should not be used")
    @SetEnvironmentVariable(key = "MP_MESSAGING_CONNECTOR_SOME_CONNECTOR_ATTR3", value = "used")
    public void testPropertyNames() {
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
                return 200;
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
        SmallRyeConfig c = builder.build();

        ConnectorConfig result = new ConnectorConfig("mp.messaging.incoming.", c, "foo");
        Iterable<String> names = result.getPropertyNames();
        assertThat(names)
                .containsExactlyInAnyOrder("connector", "ATTR1", "attr2", "attr.2", "ATTR", "AT_TR", "key", "SOME_KEY",
                        "SOME_OTHER_KEY", "ATTR3", "channel-name");

        assertThat(result.getOptionalValue("connector", String.class)).hasValue("some-connector");
        assertThat(result.getOptionalValue("attr1", String.class)).hasValue("value");
        assertThat(result.getOptionalValue("attr2", Integer.class)).hasValue(23);
        assertThat(result.getOptionalValue("attr.2", String.class)).hasValue("test");
        assertThat(result.getOptionalValue("at-tr", String.class)).hasValue("another-value");
        assertThat(result.getOptionalValue("AT_TR", String.class)).hasValue("another-value");
        assertThat(result.getOptionalValue("some-key", String.class)).hasValue("another-value-from-connector");
        assertThat(result.getOptionalValue("SOME_KEY", String.class)).hasValue("another-value-from-connector");
        assertThat(result.getOptionalValue("key", String.class)).hasValue("value");
        assertThat(result.getOptionalValue("attr3", String.class)).hasValue("used");
        assertThat(result.getOptionalValue("ATTR3", String.class)).hasValue("used");
    }

}
