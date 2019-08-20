package io.smallrye.reactive.messaging.http;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HttpConnectorConfig {
    private final Map<String, Object> map;
    private final String prefix;

    public HttpConnectorConfig(String name, String type, String url) {
        map = new HashMap<>();
        prefix = "mp.messaging." + type + "." + name + ".";
        map.put(prefix + "connector", HttpConnector.CONNECTOR_NAME);
        if (url != null) {
            map.put(prefix + "url", url);
        }
    }

    public HttpConnectorConfig(String name, Map<String, Object> conf) {
        prefix = "mp.messaging.outgoing." + name + ".";
        this.map = conf;
        map.put(prefix + "connector", HttpConnector.CONNECTOR_NAME);
    }

    public HttpConnectorConfig converter(String className) {
        map.put(prefix + "converter", className);
        return this;
    }

    void write() {
        File out = new File("target/test-classes/META-INF/microprofile-config.properties");
        if (out.isFile()) {
            out.delete();
        }
        out.getParentFile().mkdirs();

        Properties properties = new Properties();
        map.forEach((key, value) -> properties.setProperty(key, value.toString()));
        try (FileOutputStream fos = new FileOutputStream(out)) {
            properties.store(fos, "file generated for testing purpose");
            fos.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
