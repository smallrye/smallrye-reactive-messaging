package io.smallrye.reactive.messaging.camel;

import java.io.File;
import java.util.Arrays;

import org.apache.camel.CamelContext;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class CamelTestBase {

    private Weld weld;
    protected WeldContainer container;

    @BeforeEach
    public void init() {
        weld = new Weld();
        weld.addBeanClass(CamelCdiLite.class);
        clear();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @AfterEach
    public void cleanUp() {
        weld.shutdown();
        clear();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    public void initialize() {
        container = weld.initialize();
    }

    public void addClasses(Class<?>... classes) {
        Arrays.stream(classes).forEach(weld::addBeanClass);
    }

    public static void addConfig(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            clear();
        }
    }

    public static void clear() {
        File out = new File("target/test-classes/META-INF/microprofile-config.properties");
        if (out.isFile()) {
            //noinspection ResultOfMethodCallIgnored
            out.delete();
        }
    }

    public CamelContext camelContext() {
        return bean(CamelContext.class);
    }

    public <T> T bean(Class<T> clazz) {
        return container.getBeanManager().createInstance().select(clazz).get();
    }

}
