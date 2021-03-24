package io.smallrye.reactive.messaging.test.common.config;

import static io.smallrye.reactive.messaging.test.common.config.MapBasedConfig.TEST_MP_CFG_PROPERTIES;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Collections;

import org.assertj.core.util.Streams;
import org.junit.jupiter.api.Test;

class MapBasedConfigTest {

    @Test
    void testCreationFromMap() {
        MapBasedConfig config = new MapBasedConfig(Collections.singletonMap("k", "v"));
        assertTrue(config.containsKey("k"));
        assertEquals("v", config.get("k"));
        assertEquals("v", config.getValue("k", String.class));

        assertNotSame(config, config.copy());
        assertSame(config, config.getMap());

        assertEquals(1, Streams.stream(config.getPropertyNames()).count());
    }

    @Test
    void testEmpty() {
        assertFalse(new MapBasedConfig().getOptionalValue("missing", String.class).isPresent());
    }

    @Test
    void testWith() {
        MapBasedConfig config = new MapBasedConfig()
                .with("k", "v")
                .with("k2", 2);

        assertEquals(2, config.size());
        assertEquals("v", config.getValue("k", String.class));
        assertEquals(2, config.getValue("k2", Integer.class));
        assertEquals(2, Streams.stream(config.getPropertyNames()).count());

        config = config.without("k2");
        assertEquals(1, config.size());
        assertEquals("v", config.getValue("k", String.class));
        assertFalse(config.getOptionalValue("k2", Integer.class).isPresent());
    }

    @Test
    void testConfigImplementation() {
        MapBasedConfig config = new MapBasedConfig()
                .with("k", "v")
                .with("k2", 2);

        assertEquals(0, Streams.stream(config.getConfigSources()).count());
        assertEquals(3, config.getOptionalValue("missing", Integer.class).orElse(3));
        assertEquals(2, config.getOptionalValue("k2", Integer.class).orElse(3));
    }

    @Test
    void testFileBackend() {
        MapBasedConfig.cleanup();
        MapBasedConfig config = new MapBasedConfig()
                .with("k", "v")
                .with("k2", 2);
        File out = new File(TEST_MP_CFG_PROPERTIES);
        assertFalse(out.isFile());
        config.write();
        assertTrue(out.isFile());
        config.write();
        assertTrue(out.isFile());
        MapBasedConfig.cleanup();
        assertFalse(out.isFile());
    }

}
