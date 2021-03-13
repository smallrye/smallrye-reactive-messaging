package io.smallrye.reactive.messaging.kafka.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;

class ConfigHelperTest {

    @Test
    public void testMergedConfig() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put("test", "test");
        cfg.put("ov-2", "ov-cfg");

        Map<String, Object> channelSpecific = new HashMap<>();
        channelSpecific.put("spec-1", "spec");
        channelSpecific.put("spec-2", 2);
        channelSpecific.put("ov", "ov-spec");
        channelSpecific.put("ov-2", "ov-spec");

        Map<String, Object> global = new HashMap<>();
        global.put("global-1", "global");
        global.put("global-2", 3);
        global.put("ov", "ov-global");
        global.put("ov-2", "ov-global");

        SmallRyeConfigBuilder test = new SmallRyeConfigBuilder()
                .withSources(new PropertiesConfigSource(cfg, "test", 500));

        Config config = ConfigHelper.merge(test.build(), channelSpecific, global);

        assertThat(config.getValue("test", String.class)).isEqualTo("test");
        Assertions.assertThatThrownBy(() -> config.getValue("test", Double.class))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(config.getOptionalValue("test", String.class)).hasValue("test");

        assertThat(config.getOptionalValue("spec-1", String.class)).hasValue("spec");
        assertThat(config.getValue("spec-1", String.class)).isEqualTo("spec");
        Assertions.assertThatThrownBy(() -> config.getValue("spec-1", Double.class))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(config.getValue("spec-2", Integer.class)).isEqualTo(2);

        assertThat(config.getOptionalValue("global-1", String.class)).hasValue("global");
        assertThat(config.getValue("global-1", String.class)).isEqualTo("global");
        Assertions.assertThatThrownBy(() -> config.getValue("global-1", Double.class))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(config.getValue("global-2", Integer.class)).isEqualTo(3);

        assertThat(config.getOptionalValue("ov", String.class)).hasValue("ov-spec");
        assertThat(config.getOptionalValue("ov-2", String.class)).hasValue("ov-cfg");

        assertThat(config.getOptionalValue("missing", String.class)).isEmpty();
        assertThatThrownBy(() -> config.getValue("missing", Integer.class)).isInstanceOf(NoSuchElementException.class);

        assertThat(config.getConfigValue("test").getValue()).isEqualTo("test");
        assertThat(config.getConfigValue("global-1").getValue()).isNull();

        assertThat(config.getPropertyNames())
                .containsExactlyInAnyOrder("test", "ov", "ov-2", "global-1", "global-2", "spec-1", "spec-2");

        assertThat(config.getConfigSources()).hasSize(2);
    }

    @Test
    public void testMergeWithoutOtherMaps() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put("test", "test");
        Config test = new SmallRyeConfigBuilder()
                .withSources(new PropertiesConfigSource(cfg, "test", 500))
                .build();

        Config config = ConfigHelper.merge(test, Collections.emptyMap(), Collections.emptyMap());
        assertThat(config).isEqualTo(test);
        assertThat(config.getValue("test", String.class)).isEqualTo("test");

        assertThat(config.getConfigSources()).hasSize(2);
    }

}
