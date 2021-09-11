package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.annotation.Annotation;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.enterprise.inject.spi.Extension;

import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.extension.ChannelProducer;
import io.smallrye.reactive.messaging.extension.HealthCenter;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.impl.LegacyConfiguredChannelFactory;
import io.smallrye.reactive.messaging.metrics.MetricDecorator;
import io.smallrye.reactive.messaging.wiring.Wiring;

public class WeldTestBaseWithoutTails {

    static final List<String> EXPECTED = Multi.createFrom().range(1, 11).flatMap(i -> Multi.createFrom().items(i, i))
            .map(i -> Integer.toString(i))
            .collect().asList()
            .await().indefinitely();

    protected SeContainerInitializer initializer;

    protected SeContainer container;

    @BeforeAll
    public static void disableLogging() {
        System.setProperty("java.util.logging.config.file", "logging.properties");
    }

    public static void releaseConfig() {
        SmallRyeConfigProviderResolver.instance()
                .releaseConfig(ConfigProvider.getConfig(WeldTestBaseWithoutTails.class.getClassLoader()));
        clearConfigFile();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void clearConfigFile() {
        File out = new File("target/test-classes/META-INF/microprofile-config.properties");
        if (out.isFile()) {
            out.delete();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void installConfig(String path) {
        releaseConfig();
        File file = new File(path);
        if (file.isFile()) {
            File out = new File("target/test-classes/META-INF/microprofile-config.properties");
            if (out.isFile()) {
                out.delete();
            }
            out.getParentFile().mkdirs();
            try {
                Files.copy(file.toPath(), out.toPath());
                System.out.println("Installed configuration:");
                List<String> list = Files.readAllLines(out.toPath());
                list.forEach(System.out::println);
                System.out.println("---------");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            throw new IllegalArgumentException("File " + file.getAbsolutePath() + " does not exist " + path);
        }
    }

    @BeforeEach
    public void setUp() {
        initializer = SeContainerInitializer.newInstance();

        initializer.addBeanClasses(MediatorFactory.class,
                Wiring.class,
                ExecutionHolder.class,
                MediatorManager.class,
                WorkerPoolRegistry.class,
                InternalChannelRegistry.class,
                ChannelProducer.class,
                ConfiguredChannelFactory.class,
                LegacyConfiguredChannelFactory.class,
                MetricDecorator.class,
                HealthCenter.class,

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
        return installInitializeAndGet(beanClass, true);
    }

    protected <T> T installInitializeAndGet(Class<T> beanClass, boolean assertNoWiringErrors) {
        initializer.addBeanClasses(beanClass);
        initialize();

        if (assertNoWiringErrors) {
            assertNoWiringErrors();
        }

        return get(beanClass);
    }

    private void assertNoWiringErrors() {
        Wiring wiring = get(Wiring.class);
        assertThat(wiring.getGraph().hasWiringErrors()).isFalse();
    }

    protected <T> T get(Class<T> c) {
        return container.getBeanManager().createInstance().select(c).get();
    }

    protected <T> T get(Class<T> c, Annotation... qualifiers) {
        return container.getBeanManager().createInstance().select(c, qualifiers).get();
    }
}
