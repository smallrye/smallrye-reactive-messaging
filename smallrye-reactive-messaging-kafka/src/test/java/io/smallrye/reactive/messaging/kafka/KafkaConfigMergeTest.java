package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.reactive.messaging.kafka.impl.ConfigHelper;

public class KafkaConfigMergeTest {

    @Test
    public void testConfigMerge() {
        Map<String, Object> globalMap = new HashMap<>();
        globalMap.put("a", "string");
        globalMap.put("b", 23);
        globalMap.put("c", "some-value");

        Map<String, String> conf = new HashMap<>();
        globalMap.put("d", "string");
        globalMap.put("e", 23);
        globalMap.put("c", "some-value-from-conf");

        SmallRyeConfig config = new SmallRyeConfigBuilder()
                .addDefaultSources()
                .withSources(new ConfigSource() {
                    @Override
                    public Set<String> getPropertyNames() {
                        return conf.keySet();
                    }

                    @Override
                    public String getValue(String propertyName) {
                        return conf.get(propertyName);
                    }

                    @Override
                    public String getName() {
                        return "internal";
                    }
                }).build();

        Config merged = ConfigHelper.merge(config, globalMap);
        assertThat(merged.getValue("a", String.class)).isEqualTo("string");
        assertThat(merged.getOptionalValue("a", String.class)).contains("string");
        assertThat(merged.getValue("b", Integer.class)).isEqualTo(23);
        assertThat(merged.getOptionalValue("b", Integer.class)).contains(23);

        assertThat(merged.getValue("d", String.class)).isEqualTo("string");
        assertThat(merged.getOptionalValue("d", String.class)).contains("string");
        assertThat(merged.getValue("e", Integer.class)).isEqualTo(23);
        assertThat(merged.getOptionalValue("e", Integer.class)).contains(23);

        assertThat(merged.getValue("c", String.class)).isEqualTo("some-value-from-conf");
        assertThat(merged.getOptionalValue("c", String.class)).contains("some-value-from-conf");

        assertThatThrownBy(() -> merged.getValue("missing", String.class)).isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(() -> merged.getValue("missing", Integer.class)).isInstanceOf(NoSuchElementException.class);
        assertThat(merged.getOptionalValue("missing", String.class)).isEmpty();
        assertThat(merged.getOptionalValue("missing", Integer.class)).isEmpty();

        assertThatThrownBy(() -> merged.getValue("a", Integer.class)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> merged.getValue("d", Integer.class)).isInstanceOf(IllegalArgumentException.class);
    }
}
