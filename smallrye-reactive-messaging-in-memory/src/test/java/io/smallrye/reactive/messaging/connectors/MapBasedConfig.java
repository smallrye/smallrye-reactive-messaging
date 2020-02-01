package io.smallrye.reactive.messaging.connectors;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.*;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;

/**
 * An implementation of {@link Config} based on a simple {@link Map}.
 * This class is just use to mock real configuration, so should only be used for tests.
 * <p>
 * Note that this implementation does not do any conversion, so you must pass the expected object instances.
 */
public class MapBasedConfig implements Config {
    protected static final String CONFIG_FILE_PATH = "target/test-classes/META-INF/microprofile-config.properties";
    private final Map<String, Object> map;

    public MapBasedConfig(Map<String, Object> map) {
        this.map = map;
    }

    @Override
    public <T> T getValue(String propertyName, Class<T> propertyType) {
        return getOptionalValue(propertyName, propertyType).orElseThrow(() -> new NoSuchElementException(propertyName));
    }

    @Override
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
        @SuppressWarnings("unchecked")
        T value = (T) map.get(propertyName);
        return Optional.ofNullable(value);
    }

    @Override
    public Iterable<String> getPropertyNames() {
        return map.keySet();
    }

    @Override
    public Iterable<ConfigSource> getConfigSources() {
        return Collections.emptyList();
    }

    public void write() {
        File out = new File(CONFIG_FILE_PATH);
        if (out.isFile()) {
            out.delete();
        }
        out.getParentFile().mkdirs();

        Properties properties = new Properties();
        map.forEach((key, value) -> properties.setProperty(key, value.toString()));
        try (FileOutputStream fos = new FileOutputStream(out)) {
            properties.store(fos, "file generated for testing purpose");
            fos.flush();
            System.out.println("Installed configuration:");
            List<String> list = Files.readAllLines(out.toPath());
            list.forEach(System.out::println);
            System.out.println("---------");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }
}
