package io.smallrye.reactive.messaging.providers.connectors;

import java.io.File;
import java.util.Collections;
import java.util.List;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import jakarta.enterprise.inject.spi.Extension;

import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.providers.MediatorFactory;
import io.smallrye.reactive.messaging.providers.extension.ChannelProducer;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.providers.extension.MediatorManager;
import io.smallrye.reactive.messaging.providers.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.providers.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.providers.impl.ConnectorFactories;
import io.smallrye.reactive.messaging.providers.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.providers.metrics.MetricDecorator;
import io.smallrye.reactive.messaging.providers.metrics.MicrometerDecorator;
import io.smallrye.reactive.messaging.providers.wiring.Wiring;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class WeldTestBase {

    protected SeContainerInitializer initializer;

    protected SeContainer container;

    @BeforeAll
    public static void disableLogging() {
        System.setProperty("java.util.logging.config.file", "logging.properties");
    }

    public static void releaseConfig() {
        SmallRyeConfigProviderResolver.instance()
                .releaseConfig(ConfigProvider.getConfig(WeldTestBase.class.getClassLoader()));
        clearConfigFile();
    }

    private static void clearConfigFile() {
        File out = new File("target/test-classes/META-INF/microprofile-config.properties");
        if (out.isFile()) {
            out.delete();
        }
    }

    public static void installConfig(MapBasedConfig config) {
        releaseConfig();
        if (config != null) {
            config.write();
        } else {
            clearConfigFile();
        }
    }

    @BeforeEach
    public void setUp() {
        initializer = SeContainerInitializer.newInstance();

        initializer.addBeanClasses(MediatorFactory.class,
                MediatorManager.class,
                WorkerPoolRegistry.class,
                ExecutionHolder.class,
                InternalChannelRegistry.class,
                ChannelProducer.class,
                ConnectorFactories.class,
                ConfiguredChannelFactory.class,
                MetricDecorator.class,
                MicrometerDecorator.class,
                HealthCenter.class,
                Wiring.class,

                // In memory connector
                InMemoryConnector.class,

                // SmallRye config
                io.smallrye.config.inject.ConfigProducer.class);

        List<Class<?>> beans = getBeans();
        initializer.addBeanClasses(beans.toArray(new Class<?>[0]));
        initializer.disableDiscovery();
        initializer.addExtensions(new ReactiveMessagingExtension());
    }

    public List<Class<?>> getBeans() {
        return Collections.emptyList();
    }

    @AfterEach
    public void tearDown() {
        if (container != null) {
            container.close();
            container = null;
        }
    }

    protected ChannelRegistry registry(SeContainer container) {
        return container.select(ChannelRegistry.class).get();
    }

    public void addBeanClass(Class<?>... beanClass) {
        initializer.addBeanClasses(beanClass);
    }

    @SafeVarargs
    public final void addExtensionClass(Class<? extends Extension>... extensionClasses) {
        initializer.addExtensions(extensionClasses);
    }

    public void initialize() {
        assert container == null;
        container = initializer.initialize();
    }

    protected <T> T installInitializeAndGet(Class<T> beanClass) {
        initializer.addBeanClasses(beanClass);
        initialize();
        return get(beanClass);
    }

    protected <T> T get(Class<T> c) {
        return container.getBeanManager().createInstance().select(c).get();
    }
}
