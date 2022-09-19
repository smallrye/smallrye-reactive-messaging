package io.smallrye.reactive.messaging.camel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@ApplicationScoped
public class MySink {

    private final List<String> list = new ArrayList<>();

    @Incoming("data")
    public void consume(String content) {
        list.add(content);
    }

    @Produces
    public Config myConfig() {
        String prefix = "smallrye.messaging.source.data.";
        Map<String, Object> config = new HashMap<>();
        config.putIfAbsent(prefix + "name", "foo-out");
        config.put(prefix + "connector", CamelConnector.CONNECTOR_NAME);
        return new MapBasedConfig(config);
    }

    public List<String> list() {
        return list;
    }
}
