package io.smallrye.reactive.messaging.camel.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.smallrye.reactive.messaging.camel.CamelConnector;
import io.smallrye.reactive.messaging.camel.CamelTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class CamelSinkTest extends CamelTestBase {

    private final Path path = new File("target/values.txt").toPath();

    @Before
    public void setup() throws IOException {
        Files.deleteIfExists(path);
    }

    @After
    public void tearDown() throws IOException {
        Files.deleteIfExists(path);
    }

    @Test
    public void testUsingRegularEndpointAsSink() {
        addConfig(getConfigUsingRegularEndpoint());
        addClasses(BeanWithCamelSinkUsingRegularEndpoint.class);
        initialize();
        assertFileContent();
    }

    public MapBasedConfig getConfigUsingRegularEndpoint() {
        String prefix = "mp.messaging.outgoing.data.";
        Map<String, Object> config = new HashMap<>();
        config.putIfAbsent(prefix + "endpoint-uri", "file:./target?fileName=values.txt&fileExist=append");
        config.put(prefix + "connector", CamelConnector.CONNECTOR_NAME);
        return new MapBasedConfig(config);
    }

    private void assertFileContent() {
        await().until(() -> {
            if (!path.toFile().isFile()) {
                return false;
            }
            List<String> list = Files.readAllLines(path);
            return list.size() == 1 && list.get(0).equalsIgnoreCase("abcd");
        });
    }

    @Test
    public void testUsingRegularRouteAsSink() {
        addClasses(BeanWithCamelSinkUsingRegularRoute.class);
        addConfig(getConfigUsingRoute());
        initialize();

        assertFileContent();
        BeanWithCamelSinkUsingRegularRoute bean = container.getBeanManager()
                .createInstance().select(BeanWithCamelSinkUsingRegularRoute.class).get();
        assertThat(bean.getList()).hasSize(4).allSatisfy(map -> assertThat(map).contains(entry("key", "value")));
    }

    private MapBasedConfig getConfigUsingRoute() {
        String prefix = "mp.messaging.outgoing.data.";
        Map<String, Object> config = new HashMap<>();
        config.putIfAbsent(prefix + "endpoint-uri", "seda:in");
        config.put(prefix + "connector", CamelConnector.CONNECTOR_NAME);
        return new MapBasedConfig(config);
    }

    @Test
    public void testUsingRSRouteAsSink() {
        addClasses(BeanWithCamelSinkUsingRSRoute.class);
        addConfig(getConfigUsingRS());
        initialize();

        assertFileContent();
    }

    private MapBasedConfig getConfigUsingRS() {
        String prefix = "mp.messaging.outgoing.data.";
        Map<String, Object> config = new HashMap<>();
        config.putIfAbsent(prefix + "endpoint-uri", "reactive-streams:in");
        config.put(prefix + "connector", CamelConnector.CONNECTOR_NAME);
        return new MapBasedConfig(config);
    }

}
